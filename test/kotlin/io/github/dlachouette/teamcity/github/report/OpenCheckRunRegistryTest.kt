package io.github.dlachouette.teamcity.github.report

import io.github.dlachouette.teamcity.github.api.CheckRunStatus
import io.github.dlachouette.teamcity.github.testsupport.LoggerBootstrap
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path

class OpenCheckRunRegistryTest {

    init { LoggerBootstrap.install() }

    @Test
    fun `legacy rows survive reload and migrate to the current schema`(@TempDir tmp: Path) {
        val file = File(tmp.toFile(), "sub/open.tsv").apply {
            parentFile.mkdirs()
            writeText("1\tBt_A\tabc\t1000\n2\tBt_B\tdef\t2000\n")
        }
        val registry = OpenCheckRunRegistry(file)
        assertTrue(registry.load())
        assertEquals(
            listOf(
                OpenCheckRunRegistry.Entry(1, "Bt_A", "abc", 1_000L),
                OpenCheckRunRegistry.Entry(2, "Bt_B", "def", 2_000L),
            ),
            registry.snapshot(),
        )
        assertTrue(registry.flush())
        assertTrue(file.readText().startsWith("v2\t"))

        val reloaded = OpenCheckRunRegistry(file, now = { 123 }).also { assertTrue(it.load()) }
        assertEquals(registry.snapshot(), reloaded.snapshot())
        assertEquals(CheckRunCreatePhase.LEGACY, reloaded.entry(1)?.createPhase)
    }

    @Test
    fun `missing registry is a valid empty state`(@TempDir tmp: Path) {
        val registry = OpenCheckRunRegistry(tmp.resolve("missing.tsv").toFile())
        assertTrue(registry.load())
        assertTrue(registry.available)
        assertTrue(registry.snapshot().isEmpty())
    }

    @Test
    fun `missing registry with an initialized marker disables writes`(@TempDir tmp: Path) {
        val file = tmp.resolve("state.tsv").toFile()
        val first = OpenCheckRunRegistry(file)
        assertTrue(first.save(OpenCheckRunRegistry.Entry(1, "Bt", "sha", 1)))
        assertTrue(first.flush())
        assertTrue(file.delete())

        val reloaded = OpenCheckRunRegistry(file)
        assertFalse(reloaded.load())
        assertFalse(reloaded.available)
        assertFalse(reloaded.save(OpenCheckRunRegistry.Entry(2, "Bt_2", "sha-2", 2)))
    }

    @Test
    fun `corrupt rows preserve readable entries but disable writes`(@TempDir tmp: Path) {
        val file = File(tmp.toFile(), "bad.tsv").apply {
            writeText("x\ty\n1\tBt\tsha\tnot-a-time\n7\tBt\tsha\t5\n")
        }
        val original = file.readText()
        val registry = OpenCheckRunRegistry(file).also { assertFalse(it.load()) }

        assertFalse(registry.available)
        assertEquals(listOf(7L), registry.snapshot().map { it.promotionId })
        assertFalse(registry.save(OpenCheckRunRegistry.Entry(8, "Bt_8", "sha-8", 10)))
        assertFalse(registry.flush())
        assertEquals(original, file.readText())
    }

    @Test
    fun `identity state and completed high water survive a restart`(@TempDir tmp: Path) {
        val file = tmp.resolve("state.tsv").toFile()
        val identity = CheckRunIdentity(9, "Bt_9", "owner/repo", "TeamCity / Build", "sha-9")
        val entry = OpenCheckRunRegistry.Entry(
            promotionId = identity.promotionId,
            buildTypeExternalId = identity.buildTypeExternalId,
            headSha = identity.headSha,
            openedAt = 123,
            repoSlug = identity.repoSlug,
            checkName = identity.checkName,
            externalId = "tcgh:9:registry-test",
            checkRunId = 9876543210L,
            desiredStatus = CheckRunStatus.COMPLETED,
            confirmedStatus = CheckRunStatus.COMPLETED,
            createPhase = CheckRunCreatePhase.COMPLETED,
        )
        val registry = OpenCheckRunRegistry(file, now = { 123 })
        assertTrue(registry.save(entry))
        assertTrue(registry.flush())

        val reloaded = OpenCheckRunRegistry(file, now = { 123 }).also { assertTrue(it.load()) }
        assertEquals(entry, reloaded.entry(9))
        assertTrue(reloaded.openEntries().isEmpty())
    }

    @Test
    fun `completed promotion retention is bounded by age and count`(@TempDir tmp: Path) {
        var now = 1_000L
        val file = tmp.resolve("bounded.tsv").toFile()
        val registry = OpenCheckRunRegistry(file, now = { now }, terminalRetentionMs = 100, maxTerminalEntries = 2)
        listOf(
            completedEntry(1, openedAt = 910),
            completedEntry(2, openedAt = 920),
            completedEntry(3, openedAt = 930),
        ).forEach {
            assertTrue(registry.save(it))
            assertTrue(registry.flush())
        }

        assertEquals(listOf(2L, 3L), registry.snapshot().map { it.promotionId }.sorted())
        val reloaded = OpenCheckRunRegistry(file, now = { now }, terminalRetentionMs = 100, maxTerminalEntries = 2)
            .also { assertTrue(it.load()) }
        assertEquals(listOf(2L, 3L), reloaded.snapshot().map { it.promotionId }.sorted())

        now = 1_100L
        assertTrue(reloaded.save(OpenCheckRunRegistry.Entry(4, "Bt_4", "sha-4", now)))
        assertTrue(reloaded.flush())
        val afterExpiry = OpenCheckRunRegistry(file, now = { now }, terminalRetentionMs = 100, maxTerminalEntries = 2)
            .also { assertTrue(it.load()) }
        assertEquals(listOf(4L), afterExpiry.snapshot().map { it.promotionId })
    }

    @Test
    fun `registry appends changes and compacts the journal without losing the latest state`(@TempDir tmp: Path) {
        val file = tmp.resolve("journal.tsv").toFile()
        val registry = OpenCheckRunRegistry(file, now = { 1_000 })
        for (id in 1L..OpenCheckRunRegistry.MAX_JOURNAL_EVENTS.toLong()) {
            assertTrue(registry.save(OpenCheckRunRegistry.Entry(id, "Bt_$id", "sha-$id", id)))
            assertTrue(registry.flush())
        }

        assertEquals(OpenCheckRunRegistry.MAX_JOURNAL_EVENTS, file.readLines().count { it.startsWith("v2\t") })
        assertTrue(file.readLines().last().startsWith("v3\tN\t"), "the snapshot preserves its installation namespace")

        assertTrue(registry.save(OpenCheckRunRegistry.Entry(1, "Bt_1", "updated-sha", 1)))
        assertTrue(registry.flush())
        assertTrue(file.readLines().last().startsWith("v3\tU\t"), "a single changed row is appended")

        val reloaded = OpenCheckRunRegistry(file, now = { 1_000 }).also { assertTrue(it.load()) }
        assertEquals(OpenCheckRunRegistry.MAX_JOURNAL_EVENTS, reloaded.size())
        assertEquals("updated-sha", reloaded.entry(1)?.headSha)
    }

    @Test
    fun `correlation namespace survives restart and is distinct for another registry`(@TempDir tmp: Path) {
        val file = tmp.resolve("runs.tsv").toFile()
        val first = OpenCheckRunRegistry(file)
        assertTrue(first.load())
        val namespace = first.correlationNamespace()
        assertTrue(namespace != null)
        assertTrue(first.flush())

        val afterRestart = OpenCheckRunRegistry(file).also { assertTrue(it.load()) }
        val separate = OpenCheckRunRegistry(tmp.resolve("other.tsv").toFile()).also { assertTrue(it.load()) }
        assertEquals(namespace, afterRestart.correlationNamespace())
        assertTrue(namespace != separate.correlationNamespace())
    }

    private fun completedEntry(id: Long, openedAt: Long) = OpenCheckRunRegistry.Entry(
        promotionId = id,
        buildTypeExternalId = "Bt_$id",
        headSha = "sha-$id",
        openedAt = openedAt,
        repoSlug = "owner/repo",
        checkName = "TeamCity / Build",
        externalId = "tcgh:test:$id",
        checkRunId = 10_000 + id,
        desiredStatus = CheckRunStatus.COMPLETED,
        confirmedStatus = CheckRunStatus.COMPLETED,
        createPhase = CheckRunCreatePhase.COMPLETED,
    )
}
