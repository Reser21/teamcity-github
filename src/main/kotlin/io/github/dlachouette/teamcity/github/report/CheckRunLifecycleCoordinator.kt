package io.github.dlachouette.teamcity.github.report

import com.intellij.openapi.diagnostic.Logger
import io.github.dlachouette.teamcity.github.api.CheckRunCreateResult
import io.github.dlachouette.teamcity.github.api.CheckRunLookup
import io.github.dlachouette.teamcity.github.api.CheckRunRequest
import io.github.dlachouette.teamcity.github.api.CheckRunStatus
import io.github.dlachouette.teamcity.github.api.CheckRunSummary
import io.github.dlachouette.teamcity.github.api.CheckRunUpdateResult
import io.github.dlachouette.teamcity.github.api.GitHubClient
import io.github.dlachouette.teamcity.github.api.RepoCoords
import java.security.MessageDigest

data class CheckRunIdentity(
    val promotionId: Long,
    val buildTypeExternalId: String,
    val repoSlug: String,
    val checkName: String,
    val headSha: String,
) {
    fun matches(entry: OpenCheckRunRegistry.Entry): Boolean =
        entry.promotionId == promotionId &&
            entry.buildTypeExternalId == buildTypeExternalId &&
            entry.repoSlug == repoSlug &&
            entry.checkName == checkName &&
            entry.headSha == headSha

    fun sameCheckDefinition(other: CheckRunIdentity): Boolean =
        buildTypeExternalId == other.buildTypeExternalId &&
            repoSlug == other.repoSlug &&
            checkName == other.checkName &&
            headSha == other.headSha

}

/** Serializes callbacks per promotion and owns the create-once / update-by-id lifecycle. */
class CheckRunLifecycleCoordinator(
    private val registry: OpenCheckRunRegistry,
    private val gitHubClient: GitHubClient,
) {

    fun publish(
        identity: CheckRunIdentity,
        accessToken: String,
        repo: RepoCoords,
        request: CheckRunRequest,
        apiBase: String,
        label: String,
    ): Boolean = registry.withPromotionLock(identity.promotionId) {
        publishLocked(identity, accessToken, repo, request, apiBase, label)
    }

    /** Resolve a create whose POST may have completed before the server stopped. */
    fun recover(
        identity: CheckRunIdentity,
        accessToken: String,
        repo: RepoCoords,
        apiBase: String,
    ): Boolean = registry.withPromotionLock(identity.promotionId) {
        if (!checkUsable(identity, repo)) return@withPromotionLock false
        val entry = registry.entry(identity.promotionId) ?: return@withPromotionLock false
        if (entry.createPhase == CheckRunCreatePhase.LEGACY) {
            warnLegacy(entry)
            return@withPromotionLock false
        }
        if (!identity.matches(entry)) {
            warnMismatch(identity)
            return@withPromotionLock false
        }
        if (entry.checkRunId != null) return@withPromotionLock true
        resolveUnassigned(entry, identity, accessToken, repo, apiBase, createRequest = null) != null
    }

    /** Hold a removed queued promotion until reconciliation has a real outcome. */
    fun deferUntilOutcome(identity: CheckRunIdentity): Boolean = registry.withPromotionLock(identity.promotionId) {
        if (!registry.available) {
            warnRegistryUnavailable()
            return@withPromotionLock false
        }
        val existing = registry.entry(identity.promotionId)
        if (existing == null) {
            val externalId = externalIdFor(identity) ?: run {
                LOG.warn("Cannot defer promotion ${identity.promotionId}: no stable Check Run correlation is available")
                return@withPromotionLock false
            }
            val deferred = OpenCheckRunRegistry.Entry(
                promotionId = identity.promotionId,
                buildTypeExternalId = identity.buildTypeExternalId,
                headSha = identity.headSha,
                openedAt = registry.nowMillis(),
                repoSlug = identity.repoSlug,
                checkName = identity.checkName,
                externalId = externalId,
                desiredStatus = CheckRunStatus.COMPLETED,
                createPhase = CheckRunCreatePhase.RESERVED,
            )
            return@withPromotionLock persist(deferred)
        }
        val entry = existing
        if (entry.createPhase == CheckRunCreatePhase.LEGACY) {
            warnLegacy(entry)
            return@withPromotionLock false
        }
        if (!identity.matches(entry)) {
            warnMismatch(identity)
            return@withPromotionLock false
        }
        if (entry.createPhase == CheckRunCreatePhase.COMPLETED ||
            (entry.createPhase == CheckRunCreatePhase.ABANDONED && entry.checkRunId != null)
        ) {
            return@withPromotionLock true
        }

        val externalId = entry.externalId ?: if (entry.checkRunId == null) externalIdFor(identity) else null
        if (entry.checkRunId == null && externalId == null) {
            LOG.warn("Cannot defer promotion ${identity.promotionId}: no stable Check Run correlation is available")
            return@withPromotionLock false
        }
        val phase = if (entry.checkRunId != null) {
            entry.createPhase
        } else {
            when (entry.createPhase) {
                CheckRunCreatePhase.ABANDONED,
                CheckRunCreatePhase.REJECTED,
                CheckRunCreatePhase.RETRYABLE -> CheckRunCreatePhase.RESERVED
                else -> entry.createPhase
            }
        }
        val deferred = entry.copy(
            externalId = externalId,
            desiredStatus = CheckRunStatus.COMPLETED,
            createPhase = phase,
        )
        if (deferred == entry) return@withPromotionLock true
        persist(deferred)
    }

    fun abandon(
        promotionId: Long,
        identity: CheckRunIdentity? = null,
        accessToken: String? = null,
        repo: RepoCoords? = null,
        apiBase: String = GitHubClient.DEFAULT_API_BASE,
        terminalRequest: CheckRunRequest? = null,
    ): Boolean = registry.withPromotionLock(promotionId) {
        if (!registry.available) {
            warnRegistryUnavailable()
            return@withPromotionLock false
        }
        if (identity != null && identity.promotionId != promotionId) {
            LOG.warn("Refusing to abandon promotion $promotionId using a different Check Run identity")
            return@withPromotionLock false
        }
        val entry = registry.entry(promotionId) ?: run {
            if (identity == null) return@withPromotionLock true
            return@withPromotionLock persist(
                OpenCheckRunRegistry.Entry(
                    promotionId = promotionId,
                    buildTypeExternalId = identity.buildTypeExternalId,
                    headSha = identity.headSha,
                    openedAt = registry.nowMillis(),
                    repoSlug = identity.repoSlug,
                    checkName = identity.checkName,
                    createPhase = CheckRunCreatePhase.ABANDONED,
                ),
            )
        }
        if (entry.createPhase == CheckRunCreatePhase.LEGACY) {
            warnLegacy(entry)
            return@withPromotionLock false
        }
        val expectedIdentity = identity ?: entry.toIdentity() ?: run {
            LOG.warn("Cannot abandon promotion $promotionId: its Check Run identity is incomplete; keeping it open for reconciliation")
            return@withPromotionLock false
        }
        if (!expectedIdentity.matches(entry)) {
            warnMismatch(expectedIdentity)
            return@withPromotionLock false
        }
        if (entry.createPhase == CheckRunCreatePhase.COMPLETED || entry.createPhase == CheckRunCreatePhase.ABANDONED) {
            return@withPromotionLock true
        }

        if (entry.checkRunId != null || entry.createPhase == CheckRunCreatePhase.POSTING || entry.createPhase == CheckRunCreatePhase.UNCERTAIN) {
            val token = accessToken
            val targetRepo = repo
            val request = terminalRequest
            if (token == null || targetRepo == null || request == null) {
                LOG.warn("Cannot abandon promotion $promotionId: its GitHub Check Run may already exist; keeping it open for reconciliation")
                return@withPromotionLock false
            }
            if (targetRepo.slug != expectedIdentity.repoSlug || request.name != expectedIdentity.checkName ||
                request.headSha != expectedIdentity.headSha || request.status != CheckRunStatus.COMPLETED ||
                request.conclusion == null
            ) {
                LOG.warn("Refusing to abandon promotion $promotionId with a mismatched or nonterminal Check Run request")
                return@withPromotionLock false
            }

            val assigned = if (entry.checkRunId != null) {
                entry
            } else {
                resolveUnassigned(entry, expectedIdentity, token, targetRepo, apiBase, createRequest = null)
                    ?: return@withPromotionLock false
            }
            if (assigned.checkRunId == null) {
                LOG.warn("Could not resolve a GitHub Check Run for promotion $promotionId while abandoning it; keeping it open for reconciliation")
                return@withPromotionLock false
            }
            return@withPromotionLock updateAssigned(
                assigned,
                expectedIdentity,
                token,
                targetRepo,
                request,
                apiBase,
                "abandoned",
            )
        }

        // RESERVED and explicit rejections are known not to have produced a
        // remote run. Tombstone them so a delayed queue callback cannot POST.
        if (!registry.save(entry.copy(createPhase = CheckRunCreatePhase.ABANDONED, pendingUpdate = false))) {
            warnRegistryUnavailable()
            return@withPromotionLock false
        }
        registry.flush()
    }

    private fun publishLocked(
        identity: CheckRunIdentity,
        accessToken: String,
        repo: RepoCoords,
        request: CheckRunRequest,
        apiBase: String,
        label: String,
    ): Boolean {
        if (!checkUsable(identity, repo)) return false
        if (request.name != identity.checkName || request.headSha != identity.headSha) {
            LOG.warn("Refusing $label Check Run for promotion ${identity.promotionId}: request identity does not match its registry identity")
            return false
        }

        var entry = registry.entry(identity.promotionId)
        if (entry == null) {
            val externalId = externalIdFor(identity) ?: run {
                warnRegistryUnavailable()
                return false
            }
            entry = OpenCheckRunRegistry.Entry(
                promotionId = identity.promotionId,
                buildTypeExternalId = identity.buildTypeExternalId,
                headSha = identity.headSha,
                openedAt = registry.nowMillis(),
                repoSlug = identity.repoSlug,
                checkName = identity.checkName,
                // Persist a deterministic, identity-scoped correlation before
                // POST. It lets us recover a completed run after its terminal
                // local ledger row has aged out.
                externalId = externalId,
                desiredStatus = request.status,
                createPhase = CheckRunCreatePhase.RESERVED,
            )
            if (!persist(entry)) return false
        } else if (entry.createPhase == CheckRunCreatePhase.LEGACY) {
            warnLegacy(entry)
            return false
        } else if (!identity.matches(entry)) {
            warnMismatch(identity)
            return false
        }

        if (entry.createPhase == CheckRunCreatePhase.COMPLETED) return true
        if (entry.createPhase == CheckRunCreatePhase.ABANDONED) return false

        val lastDesired = entry.desiredStatus
        val desired = maxStatus(lastDesired, request.status)
        if (desired != entry.desiredStatus) {
            entry = entry.copy(desiredStatus = desired)
            if (!persist(entry)) return false
        }
        // A stale callback can carry old output fields as well as an old
        // status. Ignore it instead of PATCHing the newer status with stale
        // queue/start details.
        if (rank(request.status) < rank(lastDesired)) return true

        if (entry.checkRunId == null) {
            val resolved = resolveUnassigned(
                entry, identity, accessToken, repo, apiBase, createRequest = request.copy(status = desired),
            )
            if (resolved == null) return false
            entry = resolved
            if (entry.createPhase == CheckRunCreatePhase.COMPLETED) return true
            if (entry.checkRunId == null) return false
            if (rank(entry.confirmedStatus) >= rank(desired)) return true
        }

        return updateAssigned(entry, identity, accessToken, repo, request.copy(status = desired), apiBase, label)
    }

    /** Lookup first. Only RESERVED and known rate-limit rejections may POST. */
    private fun resolveUnassigned(
        original: OpenCheckRunRegistry.Entry,
        identity: CheckRunIdentity,
        accessToken: String,
        repo: RepoCoords,
        apiBase: String,
        createRequest: CheckRunRequest?,
    ): OpenCheckRunRegistry.Entry? {
        val externalId = original.externalId ?: run {
            LOG.warn("Check Run identity for promotion ${identity.promotionId} has no persisted external_id; refusing to create or recover another run")
            return null
        }
        val found = gitHubClient.findCheckRunByExternalId(
            accessToken, repo, identity.headSha, identity.checkName, externalId, apiBase,
        )
        when (found) {
            is CheckRunLookup.Found -> {
                val run = found.checkRun
                if (!isExpectedRun(run, identity, externalId)) {
                    LOG.warn("Check Run lookup for promotion ${identity.promotionId} returned a mismatched identity; leaving it unchanged")
                    return null
                }
                val adopted = original.copy(
                    checkRunId = run.id,
                    desiredStatus = maxStatus(original.desiredStatus, run.status),
                    confirmedStatus = run.status,
                    createPhase = if (run.status == CheckRunStatus.COMPLETED) CheckRunCreatePhase.COMPLETED else CheckRunCreatePhase.ACTIVE,
                    pendingUpdate = false,
                )
                return adopted.takeIf(::persist)
            }
            CheckRunLookup.Ambiguous -> {
                LOG.warn("Multiple GitHub Check Runs match promotion ${identity.promotionId}; refusing to guess an id")
                return null
            }
            CheckRunLookup.Unavailable -> {
                LOG.warn("Could not resolve GitHub Check Run identity for promotion ${identity.promotionId}; no create or update was attempted")
                return null
            }
            CheckRunLookup.NotFound -> Unit
        }

        if (createRequest == null) return null
        if (original.createPhase != CheckRunCreatePhase.RESERVED && original.createPhase != CheckRunCreatePhase.RETRYABLE) {
            LOG.warn("GitHub Check Run create for promotion ${identity.promotionId} has an uncertain or rejected outcome; waiting for exact external_id recovery")
            return null
        }

        val posting = original.copy(createPhase = CheckRunCreatePhase.POSTING)
        if (!persist(posting)) return null
        return when (val result = gitHubClient.createCheckRun(
            accessToken,
            repo,
            createRequest.copy(
                name = identity.checkName,
                headSha = identity.headSha,
                status = posting.desiredStatus ?: createRequest.status,
                externalId = externalId,
            ),
            apiBase,
        )) {
            is CheckRunCreateResult.Created -> {
                val confirmed = posting.copy(
                    checkRunId = result.checkRunId,
                    confirmedStatus = posting.desiredStatus,
                    createPhase = if (posting.desiredStatus == CheckRunStatus.COMPLETED) {
                        CheckRunCreatePhase.COMPLETED
                    } else {
                        CheckRunCreatePhase.ACTIVE
                    },
                    pendingUpdate = false,
                )
                confirmed.takeIf(::persist)
            }
            is CheckRunCreateResult.Rejected -> {
                val phase = if (result.retryable) CheckRunCreatePhase.RETRYABLE else CheckRunCreatePhase.REJECTED
                persist(posting.copy(createPhase = phase))
                LOG.warn("GitHub rejected Check Run create for promotion ${identity.promotionId} with HTTP ${result.statusCode}")
                null
            }
            CheckRunCreateResult.Uncertain -> {
                persist(posting.copy(createPhase = CheckRunCreatePhase.UNCERTAIN))
                null
            }
        }
    }

    private fun updateAssigned(
        original: OpenCheckRunRegistry.Entry,
        identity: CheckRunIdentity,
        accessToken: String,
        repo: RepoCoords,
        request: CheckRunRequest,
        apiBase: String,
        label: String,
    ): Boolean {
        val checkRunId = original.checkRunId ?: return false
        val externalId = original.externalId ?: return false
        val desired = maxStatus(original.desiredStatus, request.status)

        if (original.pendingUpdate) {
            val remote = gitHubClient.getCheckRun(accessToken, repo, checkRunId, apiBase)
            if (remote == null || !isExpectedRun(remote, identity, externalId)) {
                LOG.warn("Could not verify the last Check Run update for promotion ${identity.promotionId}; no PATCH was retried")
                return false
            }
            if (rank(remote.status) >= rank(desired)) {
                val verified = original.copy(
                    confirmedStatus = remote.status,
                    pendingUpdate = false,
                    createPhase = if (remote.status == CheckRunStatus.COMPLETED) CheckRunCreatePhase.COMPLETED else CheckRunCreatePhase.ACTIVE,
                )
                return persist(verified)
            }
        }

        if (desired == CheckRunStatus.COMPLETED && original.confirmedStatus == CheckRunStatus.COMPLETED) {
            return persist(original.copy(createPhase = CheckRunCreatePhase.COMPLETED, pendingUpdate = false))
        }
        if (rank(request.status) < rank(desired)) return true

        val patching = original.copy(desiredStatus = desired, pendingUpdate = true)
        if (!persist(patching)) return false
        return when (val result = gitHubClient.updateCheckRun(accessToken, repo, checkRunId, request.copy(externalId = null), apiBase)) {
            CheckRunUpdateResult.Accepted -> {
                val confirmed = patching.copy(
                    confirmedStatus = desired,
                    pendingUpdate = false,
                    createPhase = if (desired == CheckRunStatus.COMPLETED) CheckRunCreatePhase.COMPLETED else CheckRunCreatePhase.ACTIVE,
                )
                val saved = persist(confirmed)
                if (!saved) LOG.warn("GitHub accepted the $label Check Run update for promotion ${identity.promotionId}, but its id could not be saved; the registry will retry persistence")
                saved
            }
            is CheckRunUpdateResult.Rejected -> {
                persist(patching.copy(pendingUpdate = false))
                LOG.warn("GitHub rejected the $label Check Run update for promotion ${identity.promotionId} with HTTP ${result.statusCode}")
                false
            }
            CheckRunUpdateResult.Uncertain -> {
                persist(patching)
                LOG.warn("GitHub Check Run update for promotion ${identity.promotionId} had an uncertain response; it will be verified before another PATCH")
                false
            }
        }
    }

    private fun persist(entry: OpenCheckRunRegistry.Entry): Boolean {
        if (!registry.available) {
            warnRegistryUnavailable()
            return false
        }
        if (!registry.save(entry)) {
            warnRegistryUnavailable()
            return false
        }
        if (!registry.flush()) {
            LOG.warn("Could not persist Check Run registry at ${registry.path}; no GitHub operation will continue until it can be saved")
            return false
        }
        return true
    }

    private fun checkUsable(identity: CheckRunIdentity, repo: RepoCoords): Boolean {
        if (!registry.available) {
            warnRegistryUnavailable()
            return false
        }
        if (repo.slug != identity.repoSlug) {
            LOG.warn("Refusing Check Run for promotion ${identity.promotionId}: repository does not match its stored identity")
            return false
        }
        return true
    }

    private fun isExpectedRun(run: CheckRunSummary, identity: CheckRunIdentity, externalId: String): Boolean =
        run.externalId == externalId && run.name == identity.checkName && run.headSha == identity.headSha

    private fun warnLegacy(entry: OpenCheckRunRegistry.Entry) {
        LOG.warn("Legacy Check Run entry for promotion ${entry.promotionId} has no GitHub run id or external_id; refusing to guess which run to update. Close the orphaned run if needed, then queue a new promotion")
    }

    private fun warnMismatch(identity: CheckRunIdentity) {
        LOG.warn("Check Run identity for promotion ${identity.promotionId} no longer matches its durable registry entry; refusing to reuse it")
    }

    private fun warnRegistryUnavailable() {
        LOG.warn("Check Run registry at ${registry.path} is unavailable or corrupt; refusing to create or guess a GitHub run id")
    }

    private fun OpenCheckRunRegistry.Entry.toIdentity(): CheckRunIdentity? {
        val repo = repoSlug ?: return null
        val name = checkName ?: return null
        return CheckRunIdentity(promotionId, buildTypeExternalId, repo, name, headSha)
    }

    private fun externalIdFor(identity: CheckRunIdentity): String? {
        val namespace = registry.correlationNamespace() ?: return null
        val fields = listOf(
            namespace,
            identity.promotionId.toString(),
            identity.buildTypeExternalId,
            identity.repoSlug,
            identity.checkName,
            identity.headSha,
        )
        val canonical = fields.joinToString("") { "${it.length}:$it" }
        val digest = MessageDigest.getInstance("SHA-256").digest(canonical.toByteArray(Charsets.UTF_8))
        val hex = digest.joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }
        return "tcgh:v2:$hex"
    }

    private fun maxStatus(first: CheckRunStatus?, second: CheckRunStatus): CheckRunStatus =
        first?.takeIf { rank(it) >= rank(second) } ?: second

    private fun rank(status: CheckRunStatus?): Int = when (status) {
        CheckRunStatus.QUEUED -> 0
        CheckRunStatus.IN_PROGRESS -> 1
        CheckRunStatus.COMPLETED -> 2
        null -> -1
    }

    companion object {
        private val LOG = Logger.getInstance(CheckRunLifecycleCoordinator::class.java.name)
    }
}
