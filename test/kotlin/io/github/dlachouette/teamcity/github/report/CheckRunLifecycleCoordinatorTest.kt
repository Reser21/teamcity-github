package io.github.dlachouette.teamcity.github.report

import io.github.dlachouette.teamcity.github.api.CheckRunCreateResult
import io.github.dlachouette.teamcity.github.api.CheckRunAnnotation
import io.github.dlachouette.teamcity.github.api.CheckRunAnnotationLevel
import io.github.dlachouette.teamcity.github.api.CheckRunConclusion
import io.github.dlachouette.teamcity.github.api.CheckRunLookup
import io.github.dlachouette.teamcity.github.api.CheckRunRequest
import io.github.dlachouette.teamcity.github.api.CheckRunStatus
import io.github.dlachouette.teamcity.github.api.CheckRunSummary
import io.github.dlachouette.teamcity.github.api.CheckRunUpdateResult
import io.github.dlachouette.teamcity.github.api.GitHubClient
import io.github.dlachouette.teamcity.github.api.RepoCoords
import io.github.dlachouette.teamcity.github.testsupport.LoggerBootstrap
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class CheckRunLifecycleCoordinatorTest {

    init { LoggerBootstrap.install() }

    private val repo = RepoCoords("owner", "repo")

    @Test
    fun `one promotion creates once then patches the same run through completion`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val registry = OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile())
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(41)

        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Building"), "api", "started"))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.COMPLETED, "Passed"), "api", "completed"))

        assertEquals(1, fake.creates.size)
        assertTrue(fake.creates.single().externalId?.startsWith("tcgh:v2:") == true)
        assertEquals(fake.creates.single().externalId, registry.entry(identity.promotionId)?.externalId)
        assertEquals(listOf(700L, 700L), fake.updates.map { it.checkRunId })
        assertEquals(listOf(CheckRunStatus.IN_PROGRESS, CheckRunStatus.COMPLETED), fake.updates.map { it.request.status })
        assertEquals(CheckRunCreatePhase.COMPLETED, registry.entry(identity.promotionId)?.createPhase)
        assertEquals(700L, registry.entry(identity.promotionId)?.checkRunId)
    }

    @Test
    fun `duplicate queue callbacks remain on the same id and a duplicate completion does not append annotations again`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val coordinator = CheckRunLifecycleCoordinator(OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile()), fake)
        val identity = identity(42)
        val queued = request(CheckRunStatus.QUEUED, "Queued")
        val completed = request(CheckRunStatus.COMPLETED, "Failed", CheckRunConclusion.FAILURE)

        assertTrue(coordinator.publish(identity, "token", repo, queued, "api", "queued"))
        assertTrue(coordinator.publish(identity, "token", repo, queued, "api", "queued duplicate"))
        assertTrue(coordinator.publish(identity, "token", repo, completed, "api", "completed"))
        assertTrue(coordinator.publish(identity, "token", repo, completed, "api", "completed duplicate"))

        assertEquals(1, fake.creates.size)
        assertEquals(listOf(700L, 700L), fake.updates.map { it.checkRunId })
        assertEquals(CheckRunStatus.COMPLETED, fake.updates.last().request.status)
    }

    @Test
    fun `concurrent queued and started callbacks cannot create separate runs`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val enteredCreate = CountDownLatch(1)
        val releaseCreate = CountDownLatch(1)
        fake.createHandler = {
            enteredCreate.countDown()
            assertTrue(releaseCreate.await(5, TimeUnit.SECONDS))
            CheckRunCreateResult.Created(701)
        }
        val coordinator = CheckRunLifecycleCoordinator(OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile()), fake)
        val identity = identity(43)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val queued = pool.submit<Boolean> {
                coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued")
            }
            assertTrue(enteredCreate.await(5, TimeUnit.SECONDS))
            val started = pool.submit<Boolean> {
                coordinator.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Building"), "api", "started")
            }
            releaseCreate.countDown()

            assertTrue(queued.get(5, TimeUnit.SECONDS))
            assertTrue(started.get(5, TimeUnit.SECONDS))
            assertEquals(1, fake.creates.size)
            assertEquals(listOf(701L), fake.updates.map { it.checkRunId })
            assertEquals(CheckRunStatus.IN_PROGRESS, fake.updates.single().request.status)
        } finally {
            releaseCreate.countDown()
            pool.shutdownNow()
        }
    }

    @Test
    fun `a persisted run id survives registry reload and is used for the next event`(@TempDir tmp: Path) {
        val file = tmp.resolve("runs.tsv").toFile()
        val identity = identity(44)
        val firstClient = FakeGitHubClient()
        val first = CheckRunLifecycleCoordinator(OpenCheckRunRegistry(file), firstClient)
        assertTrue(first.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))

        val reloaded = OpenCheckRunRegistry(file).also { assertTrue(it.load()) }
        assertEquals(700L, reloaded.entry(identity.promotionId)?.checkRunId)
        val afterRestart = FakeGitHubClient()
        val second = CheckRunLifecycleCoordinator(reloaded, afterRestart)
        assertTrue(second.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Building"), "api", "started"))

        assertTrue(afterRestart.creates.isEmpty())
        assertEquals(listOf(700L), afterRestart.updates.map { it.checkRunId })
    }

    @Test
    fun `an old registry row loads and migrates without guessing a GitHub id`(@TempDir tmp: Path) {
        val file = tmp.resolve("runs.tsv").toFile()
        file.writeText("45\tBt_A\tsha-45\t1000\n")
        val registry = OpenCheckRunRegistry(file).also { assertTrue(it.load()) }
        val fake = FakeGitHubClient()
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)

        assertFalse(coordinator.publish(identity(45).copy(headSha = "sha-45"), "token", repo, request(CheckRunStatus.COMPLETED, "Vanished", CheckRunConclusion.CANCELLED, headSha = "sha-45"), "api", "reconcile"))
        assertTrue(fake.creates.isEmpty())
        assertTrue(fake.updates.isEmpty())
        assertEquals(CheckRunCreatePhase.LEGACY, registry.entry(45)?.createPhase)
        assertEquals(1000L, registry.entry(45)?.openedAt)

        assertTrue(registry.flush())
        val migrated = OpenCheckRunRegistry(file).also { assertTrue(it.load()) }
        assertEquals(registry.snapshot(), migrated.snapshot())
    }

    @Test
    fun `an uncertain create is recovered by exact external id or remains closed to another post`(@TempDir tmp: Path) {
        val file = tmp.resolve("runs.tsv").toFile()
        val fake = FakeGitHubClient().apply { createHandler = { CheckRunCreateResult.Uncertain } }
        val registry = OpenCheckRunRegistry(file)
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(46)

        assertFalse(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        fake.lookupHandler = { CheckRunLookup.NotFound }
        assertFalse(coordinator.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Building"), "api", "started"))
        assertEquals(1, fake.creates.size, "an uncertain POST must never be retried blindly")

        val externalId = registry.entry(identity.promotionId)?.externalId!!
        fake.lookupHandler = { CheckRunLookup.Found(CheckRunSummary(812, externalId, identity.checkName, identity.headSha, CheckRunStatus.QUEUED)) }
        fake.updateHandler = { _, _ -> CheckRunUpdateResult.Accepted }
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Building"), "api", "started after recovery"))
        assertEquals(1, fake.creates.size)
        assertEquals(listOf(812L), fake.updates.map { it.checkRunId })
    }

    @Test
    fun `a rate limited create can retry only after lookup confirms no matching run`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient().apply {
            createHandler = { CheckRunCreateResult.Rejected(429, retryable = true) }
        }
        val coordinator = CheckRunLifecycleCoordinator(OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile()), fake)
        val identity = identity(47)

        assertFalse(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        fake.lookupHandler = { CheckRunLookup.NotFound }
        fake.createHandler = { CheckRunCreateResult.Created(813) }
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued retry"))
        assertEquals(2, fake.creates.size)
    }

    @Test
    fun `a mismatched identity cannot reuse a promotion's stored run`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val coordinator = CheckRunLifecycleCoordinator(OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile()), fake)
        val original = identity(48)
        assertTrue(coordinator.publish(original, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))

        val mismatches = listOf(
            original.copy(buildTypeExternalId = "Bt_B"),
            original.copy(repoSlug = "owner/other"),
            original.copy(checkName = "TeamCity / Other"),
            original.copy(headSha = "different-sha"),
        )
        mismatches.forEach { mismatch ->
            val mismatchRepo = if (mismatch.repoSlug == repo.slug) repo else RepoCoords("owner", "other")
            assertFalse(
                coordinator.publish(
                    mismatch,
                    "token",
                    mismatchRepo,
                    request(CheckRunStatus.IN_PROGRESS, "Building", name = mismatch.checkName, headSha = mismatch.headSha),
                    "api",
                    "mismatched",
                ),
            )
        }
        assertEquals(1, fake.creates.size)
        assertTrue(fake.updates.isEmpty())
    }

    @Test
    fun `distinct promotions reruns repositories names and shas get distinct correlations`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val coordinator = CheckRunLifecycleCoordinator(OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile()), fake)
        val first = identity(49)
        val others = listOf(
            first.copy(promotionId = 50),
            first.copy(promotionId = 51, repoSlug = "owner/other"),
            first.copy(promotionId = 52, checkName = "TeamCity / Other"),
            first.copy(promotionId = 53, headSha = "another-sha"),
        )

        assertTrue(coordinator.publish(first, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "first"))
        assertTrue(coordinator.publish(others[0], "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "rerun"))
        assertTrue(coordinator.publish(others[1], "token", RepoCoords("owner", "other"), request(CheckRunStatus.QUEUED, "Queued"), "api", "other repository"))
        assertTrue(coordinator.publish(others[2], "token", repo, request(CheckRunStatus.QUEUED, "Queued", name = others[2].checkName), "api", "other name"))
        assertTrue(coordinator.publish(others[3], "token", repo, request(CheckRunStatus.QUEUED, "Queued", headSha = others[3].headSha), "api", "other sha"))
        assertEquals(5, fake.creates.size)
        assertEquals((700L..704L).toList(), fake.createdIds)
        assertEquals(5, fake.creates.map { it.externalId }.toSet().size)
    }

    @Test
    fun `cancelled removal and vanished promotion complete the intended run`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val registry = OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile())
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val cancelled = identity(51)
        assertTrue(coordinator.publish(cancelled, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        assertTrue(coordinator.publish(cancelled, "token", repo, request(CheckRunStatus.COMPLETED, "Cancelled before start", CheckRunConclusion.CANCELLED), "api", "queue removal"))

        val vanished = identity(52)
        assertTrue(coordinator.publish(vanished, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        assertTrue(coordinator.deferUntilOutcome(vanished))
        assertTrue(coordinator.publish(vanished, "token", repo, request(CheckRunStatus.COMPLETED, "Build no longer in TeamCity", CheckRunConclusion.CANCELLED), "api", "reconciliation"))

        assertEquals(2, fake.creates.size)
        assertEquals(listOf(700L, 701L), fake.updates.map { it.checkRunId })
        assertEquals(listOf("Cancelled before start", "Build no longer in TeamCity"), fake.updates.filter { it.request.status == CheckRunStatus.COMPLETED }.map { it.request.outputTitle })
    }

    @Test
    fun `a completed cancellation cannot be regressed by late queued or started callbacks`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val registry = OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile())
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(55)

        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.COMPLETED, "Cancelled", CheckRunConclusion.CANCELLED), "api", "queue removal"))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "late queued callback"))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Building"), "api", "late started callback"))

        assertEquals(1, fake.creates.size)
        assertEquals(CheckRunStatus.COMPLETED, fake.creates.single().status)
        assertTrue(fake.updates.isEmpty())
        assertEquals(CheckRunCreatePhase.COMPLETED, registry.entry(identity.promotionId)?.createPhase)
    }

    @Test
    fun `a late queued callback cannot replace the started output`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val coordinator = CheckRunLifecycleCoordinator(OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile()), fake)
        val identity = identity(57)

        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Building"), "api", "started"))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Late queued"), "api", "stale queued"))

        assertEquals(1, fake.creates.size)
        assertEquals(listOf(CheckRunStatus.IN_PROGRESS), fake.updates.map { it.request.status })
        assertEquals("Building", fake.updates.single().request.outputTitle)
    }

    @Test
    fun `a removed promotion ignores late queued and started callbacks before terminal reconciliation`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val file = tmp.resolve("runs.tsv").toFile()
        val registry = OpenCheckRunRegistry(file)
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(56)

        assertTrue(coordinator.deferUntilOutcome(identity))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Late queued"), "api", "late queued callback"))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Late started"), "api", "late start callback"))

        assertTrue(fake.creates.isEmpty())
        assertEquals(CheckRunStatus.COMPLETED, registry.entry(identity.promotionId)?.desiredStatus)

        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.COMPLETED, "Passed"), "api", "reconciliation"))
        assertEquals(1, fake.creates.size)
        assertEquals(CheckRunStatus.COMPLETED, fake.creates.single().status)
        assertEquals(CheckRunConclusion.SUCCESS, fake.creates.single().conclusion)
        assertEquals(CheckRunCreatePhase.COMPLETED, registry.entry(identity.promotionId)?.createPhase)
    }

    @Test
    fun `a deferred equivalent success updates the original queued run after registry reload`(@TempDir tmp: Path) {
        val file = tmp.resolve("runs.tsv").toFile()
        val fake = FakeGitHubClient()
        val identity = identity(62)
        val firstRegistry = OpenCheckRunRegistry(file)
        val first = CheckRunLifecycleCoordinator(firstRegistry, fake)

        assertTrue(first.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        assertTrue(first.deferUntilOutcome(identity))
        assertTrue(fake.updates.isEmpty())

        val reloaded = OpenCheckRunRegistry(file).also { assertTrue(it.load()) }
        assertEquals(CheckRunStatus.COMPLETED, reloaded.entry(identity.promotionId)?.desiredStatus)
        val afterRestart = CheckRunLifecycleCoordinator(reloaded, fake)
        assertTrue(afterRestart.publish(identity, "token", repo, request(CheckRunStatus.COMPLETED, "Equivalent passed", CheckRunConclusion.SUCCESS), "api", "verified equivalent"))

        assertEquals(1, fake.creates.size)
        assertEquals(listOf(700L), fake.updates.map { it.checkRunId })
        assertEquals(CheckRunConclusion.SUCCESS, fake.updates.single().request.conclusion)
        assertEquals(CheckRunCreatePhase.COMPLETED, reloaded.entry(identity.promotionId)?.createPhase)
    }

    @Test
    fun `a deferred equivalent failure updates the original queued run as failure`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val registry = OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile())
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(63)

        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        assertTrue(coordinator.deferUntilOutcome(identity))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.COMPLETED, "Equivalent failed", CheckRunConclusion.FAILURE), "api", "verified equivalent"))

        assertEquals(1, fake.creates.size)
        assertEquals(listOf(700L), fake.updates.map { it.checkRunId })
        assertEquals(CheckRunConclusion.FAILURE, fake.updates.single().request.conclusion)
        assertEquals(CheckRunCreatePhase.COMPLETED, registry.entry(identity.promotionId)?.createPhase)
    }

    @Test
    fun `an explicit terminal request updates an assigned run`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val registry = OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile())
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(58)

        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        val cancelled = request(CheckRunStatus.COMPLETED, "Cancelled", CheckRunConclusion.CANCELLED)
        assertTrue(coordinator.abandon(identity.promotionId, identity, "token", repo, "api", cancelled))

        assertEquals(1, fake.creates.size)
        assertEquals(listOf(700L), fake.updates.map { it.checkRunId })
        assertEquals(CheckRunStatus.COMPLETED, fake.updates.single().request.status)
        assertEquals(CheckRunConclusion.CANCELLED, fake.updates.single().request.conclusion)
        assertEquals(CheckRunCreatePhase.COMPLETED, registry.entry(identity.promotionId)?.createPhase)
        assertTrue(registry.openEntries().isEmpty())
    }

    @Test
    fun `abandon without credentials keeps an assigned run open for reconciliation`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient()
        val registry = OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile())
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(59)
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))

        assertFalse(coordinator.abandon(identity.promotionId, identity))

        assertEquals(CheckRunCreatePhase.ACTIVE, registry.entry(identity.promotionId)?.createPhase)
        assertEquals(listOf(700L), registry.openEntries().map { it.checkRunId })
        assertTrue(fake.updates.isEmpty())
    }

    @Test
    fun `failed terminal patch leaves the queued run open for reconciliation`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient().apply {
            updateHandler = { _, request ->
                if (request.status == CheckRunStatus.COMPLETED) CheckRunUpdateResult.Rejected(422)
                else CheckRunUpdateResult.Accepted
            }
        }
        val registry = OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile())
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(61)
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))

        assertFalse(
            coordinator.abandon(
                identity.promotionId,
                identity,
                "token",
                repo,
                "api",
                request(CheckRunStatus.COMPLETED, "Cancelled", CheckRunConclusion.CANCELLED),
            ),
        )

        assertEquals(CheckRunCreatePhase.ACTIVE, registry.entry(identity.promotionId)?.createPhase)
        assertEquals(1, registry.openEntries().size)
        assertEquals(700L, registry.openEntries().single().checkRunId)
    }

    @Test
    fun `deterministic correlation recovers a completed run after its terminal row is evicted`(@TempDir tmp: Path) {
        val file = tmp.resolve("runs.tsv").toFile()
        val fake = FakeGitHubClient()
        val registry = OpenCheckRunRegistry(file, maxTerminalEntries = 0)
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(60)

        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.COMPLETED, "Passed"), "api", "completed"))
        val externalId = fake.creates.single().externalId!!
        assertEquals(null, registry.entry(identity.promotionId))

        fake.lookupHandler = {
            CheckRunLookup.Found(CheckRunSummary(700, externalId, identity.checkName, identity.headSha, CheckRunStatus.COMPLETED))
        }
        val reloaded = OpenCheckRunRegistry(file, maxTerminalEntries = 0).also { assertTrue(it.load()) }
        val afterEviction = CheckRunLifecycleCoordinator(reloaded, fake)
        assertTrue(afterEviction.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Late queued"), "api", "late queued"))

        assertEquals(1, fake.creates.size)
        assertEquals(listOf(externalId, externalId), fake.lookups)
        assertEquals(listOf(CheckRunStatus.COMPLETED), fake.updates.map { it.request.status })
    }

    @Test
    fun `failed patch retains the id and retries that patch instead of creating`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient().apply {
            updateHandler = { _, request ->
                if (request.status == CheckRunStatus.IN_PROGRESS) CheckRunUpdateResult.Rejected(422)
                else CheckRunUpdateResult.Accepted
            }
        }
        val coordinator = CheckRunLifecycleCoordinator(OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile()), fake)
        val identity = identity(53)
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        assertFalse(coordinator.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Building"), "api", "started"))
        fake.updateHandler = { _, _ -> CheckRunUpdateResult.Accepted }
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.IN_PROGRESS, "Building"), "api", "started retry"))

        assertEquals(1, fake.creates.size)
        assertEquals(listOf(700L, 700L), fake.updates.map { it.checkRunId })
    }

    @Test
    fun `uncertain completed patch is read before retry to avoid repeating annotations`(@TempDir tmp: Path) {
        val fake = FakeGitHubClient().apply { updateHandler = { _, _ -> CheckRunUpdateResult.Uncertain } }
        val registry = OpenCheckRunRegistry(tmp.resolve("runs.tsv").toFile())
        val coordinator = CheckRunLifecycleCoordinator(registry, fake)
        val identity = identity(54)
        val complete = request(CheckRunStatus.COMPLETED, "Passed").copy(
            annotations = listOf(CheckRunAnnotation("src/main.kt", 1, 1, CheckRunAnnotationLevel.WARNING, "one warning")),
        )
        assertTrue(coordinator.publish(identity, "token", repo, request(CheckRunStatus.QUEUED, "Queued"), "api", "queued"))
        assertFalse(coordinator.publish(identity, "token", repo, complete, "api", "completed"))
        val externalId = registry.entry(identity.promotionId)?.externalId!!
        fake.statusHandler = { id -> CheckRunSummary(id, externalId, identity.checkName, identity.headSha, CheckRunStatus.COMPLETED) }

        assertTrue(coordinator.publish(identity, "token", repo, complete, "api", "completed duplicate"))
        assertEquals(1, fake.creates.size)
        assertEquals(listOf(CheckRunStatus.COMPLETED), fake.updates.map { it.request.status })
        assertEquals(1, fake.gets.size)
    }

    @Test
    fun `an equivalent promotion must match the full check identity`() {
        val target = identity(64)

        assertTrue(target.sameCheckDefinition(identity(65)))
        assertFalse(target.sameCheckDefinition(identity(65).copy(buildTypeExternalId = "Bt_B")))
        assertFalse(target.sameCheckDefinition(identity(65).copy(repoSlug = "other/repo")))
        assertFalse(target.sameCheckDefinition(identity(65).copy(checkName = "Other check")))
        assertFalse(target.sameCheckDefinition(identity(65).copy(headSha = "other-sha")))
    }

    private fun identity(promotionId: Long) = CheckRunIdentity(
        promotionId = promotionId,
        buildTypeExternalId = "Bt_A",
        repoSlug = "owner/repo",
        checkName = "TeamCity / Build",
        headSha = "sha",
    )

    private fun request(
        status: CheckRunStatus,
        title: String,
        conclusion: CheckRunConclusion? = if (status == CheckRunStatus.COMPLETED) CheckRunConclusion.SUCCESS else null,
        name: String = "TeamCity / Build",
        headSha: String = "sha",
    ) = CheckRunRequest(
        name = name,
        headSha = headSha,
        status = status,
        conclusion = conclusion,
        outputTitle = title,
        outputSummary = "summary",
    )

    private class FakeGitHubClient : GitHubClient() {
        val creates = CopyOnWriteArrayList<CheckRunRequest>()
        val createdIds = CopyOnWriteArrayList<Long>()
        val updates = CopyOnWriteArrayList<UpdateCall>()
        val lookups = CopyOnWriteArrayList<String>()
        val gets = CopyOnWriteArrayList<Long>()
        private val nextRunId = AtomicLong(699)
        @Volatile var createHandler: (CheckRunRequest) -> CheckRunCreateResult = { CheckRunCreateResult.Created(nextRunId.incrementAndGet()) }
        @Volatile var updateHandler: (Long, CheckRunRequest) -> CheckRunUpdateResult = { _, _ -> CheckRunUpdateResult.Accepted }
        @Volatile var lookupHandler: (String) -> CheckRunLookup = { CheckRunLookup.NotFound }
        @Volatile var statusHandler: (Long) -> CheckRunSummary? = { null }

        override fun createCheckRun(
            accessToken: String,
            repo: RepoCoords,
            request: CheckRunRequest,
            apiBase: String,
        ): CheckRunCreateResult {
            creates += request
            val result = createHandler(request)
            if (result is CheckRunCreateResult.Created) createdIds += result.checkRunId
            return result
        }

        override fun updateCheckRun(
            accessToken: String,
            repo: RepoCoords,
            checkRunId: Long,
            request: CheckRunRequest,
            apiBase: String,
        ): CheckRunUpdateResult {
            updates += UpdateCall(checkRunId, request)
            return updateHandler(checkRunId, request)
        }

        override fun findCheckRunByExternalId(
            accessToken: String,
            repo: RepoCoords,
            headSha: String,
            checkName: String,
            externalId: String,
            apiBase: String,
        ): CheckRunLookup {
            lookups += externalId
            return lookupHandler(externalId)
        }

        override fun getCheckRun(
            accessToken: String,
            repo: RepoCoords,
            checkRunId: Long,
            apiBase: String,
        ): CheckRunSummary? {
            gets += checkRunId
            return statusHandler(checkRunId)
        }

        data class UpdateCall(val checkRunId: Long, val request: CheckRunRequest)
    }
}
