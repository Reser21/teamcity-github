package io.github.dlachouette.teamcity.github.report

import com.intellij.openapi.diagnostic.Logger
import io.github.dlachouette.teamcity.github.api.CheckRunStatus
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.Base64
import java.util.UUID

/** Durable identity and progress ledger for every lifecycle Check Run created by this server. */
class OpenCheckRunRegistry(
    private val file: File,
    private val now: () -> Long = System::currentTimeMillis,
    private val terminalRetentionMs: Long = TERMINAL_RETENTION_MS,
    private val maxTerminalEntries: Int = MAX_TERMINAL_ENTRIES,
) {

    data class Entry(
        val promotionId: Long,
        val buildTypeExternalId: String,
        val headSha: String,
        val openedAt: Long,
        val repoSlug: String? = null,
        val checkName: String? = null,
        val externalId: String? = null,
        val checkRunId: Long? = null,
        val desiredStatus: CheckRunStatus? = null,
        val confirmedStatus: CheckRunStatus? = null,
        val createPhase: CheckRunCreatePhase = CheckRunCreatePhase.LEGACY,
        val pendingUpdate: Boolean = false,
    )

    private val entries = HashMap<Long, Entry>()
    private val terminalEntryIds = linkedSetOf<Long>()
    private val pendingChanges = LinkedHashMap<Long, Entry?>()
    private var correlationNamespace: String = UUID.randomUUID().toString()
    private var namespaceDirty = true
    private val promotionLocks = Array(64) { Any() }
    private val marker = File(file.absoluteFile.parentFile, "${file.name}.initialized")
    private var journalEventCount = 0
    private var needsCompaction = false

    @Volatile
    var available: Boolean = true
        private set

    val path: String get() = file.absolutePath

    @Synchronized
    fun correlationNamespace(): String? = correlationNamespace.takeIf { available }

    fun nowMillis(): Long = now()

    fun entry(promotionId: Long): Entry? = synchronized(this) { entries[promotionId] }

    fun snapshot(): List<Entry> = synchronized(this) { entries.values.sortedBy { it.openedAt } }

    fun openEntries(): List<Entry> = snapshot().filter {
        it.createPhase != CheckRunCreatePhase.COMPLETED && it.createPhase != CheckRunCreatePhase.ABANDONED
    }

    fun size(): Int = synchronized(this) { entries.size }

    fun withPromotionLock(promotionId: Long, action: () -> Boolean): Boolean {
        val index = (promotionId xor (promotionId ushr 32)).toInt().and(Int.MAX_VALUE) % promotionLocks.size
        return synchronized(promotionLocks[index]) { action() }
    }

    @Synchronized
    fun save(entry: Entry): Boolean {
        if (!available) return false
        entries[entry.promotionId] = entry
        if (isTerminal(entry)) terminalEntryIds += entry.promotionId else terminalEntryIds.remove(entry.promotionId)
        pendingChanges[entry.promotionId] = entry
        pruneTerminalEntries()
        return true
    }

    @Synchronized
    fun load(): Boolean {
        entries.clear()
        terminalEntryIds.clear()
        pendingChanges.clear()
        journalEventCount = 0
        needsCompaction = false
        val generatedNamespace = correlationNamespace
        if (!file.exists()) {
            available = !marker.exists()
            if (!available) {
                LOG.warn("Open Check Run registry at $file is missing although its initialized marker exists; Check Run writes are paused until it is restored")
            }
            return available
        }
        val loaded = linkedMapOf<Long, Entry>()
        val baseIds = hashSetOf<Long>()
        var invalid = false
        var hasLegacy = false
        var journalStarted = false
        var loadedJournalEvents = 0
        var storedNamespace: String? = null
        try {
            file.readLines(StandardCharsets.UTF_8).forEach { line ->
                if (line.isBlank()) return@forEach
                if (line.startsWith("v3\t")) {
                    journalStarted = true
                    loadedJournalEvents++
                    when (val event = parseEvent(line)) {
                        is RegistryEvent.Upsert -> loaded[event.entry.promotionId] = event.entry
                        is RegistryEvent.Delete -> loaded.remove(event.promotionId)
                        is RegistryEvent.Namespace -> {
                            if (storedNamespace != null && storedNamespace != event.value) invalid = true
                            storedNamespace = event.value
                        }
                        null -> invalid = true
                    }
                } else {
                    if (journalStarted) invalid = true
                    val parsed = parse(line)
                    if (parsed == null || !baseIds.add(parsed.promotionId)) {
                        invalid = true
                    } else {
                        loaded[parsed.promotionId] = parsed
                        if (parsed.createPhase == CheckRunCreatePhase.LEGACY) hasLegacy = true
                    }
                }
            }
            synchronized(this) {
                entries.clear()
                entries.putAll(loaded)
                terminalEntryIds.clear()
                loaded.values.filter(::isTerminal).forEach { terminalEntryIds += it.promotionId }
                correlationNamespace = storedNamespace ?: generatedNamespace
                namespaceDirty = storedNamespace == null && !invalid
                available = !invalid
                journalEventCount = loadedJournalEvents
                // A clean load of the old four-column format is compacted to
                // the current snapshot schema at the next flush, while
                // retaining LEGACY state so it cannot be mistaken for a run id.
                needsCompaction = !invalid && (
                    hasLegacy || storedNamespace == null || loadedJournalEvents >= MAX_JOURNAL_EVENTS || file.length() >= MAX_JOURNAL_BYTES
                )
            }
            if (invalid) {
                LOG.warn("Open Check Run registry at $file contains invalid or duplicate rows; Check Run writes are paused until it is repaired")
            } else {
                pruneTerminalEntries()
            }
            return available
        } catch (e: Exception) {
            available = false
            pendingChanges.clear()
            needsCompaction = false
            LOG.warn("Could not read open Check Run registry at $file; Check Run writes are paused until it is repaired: ${e.message}")
            return false
        }
    }

    /** Append and force changed rows before network side effects. */
    @Synchronized
    fun flush(): Boolean {
        if (!available) return false
        if (!file.exists() && marker.exists()) {
            available = false
            LOG.warn("Open Check Run registry at $file disappeared after initialization; Check Run writes are paused until it is restored")
            return false
        }
        if (pendingChanges.isEmpty() && !needsCompaction && !namespaceDirty && marker.exists()) return true
        try {
            val parent = file.absoluteFile.parentFile
            parent?.mkdirs()
            if (needsCompaction) {
                compactSnapshot(parent)
                pendingChanges.clear()
                journalEventCount = 0
                needsCompaction = false
                namespaceDirty = false
            } else if (pendingChanges.isNotEmpty() || namespaceDirty) {
                val eventCount = pendingChanges.size
                val events = buildString {
                    if (namespaceDirty) append("v3\tN\t$correlationNamespace\n")
                    pendingChanges.entries.forEach { (promotionId, entry) ->
                        append(if (entry == null) "v3\tD\t$promotionId\n" else encodeEvent(entry))
                    }
                }
                val separator = if (file.exists() && file.length() > 0 && !endsWithNewline(file)) "\n" else ""
                val bytes = (separator + events).toByteArray(StandardCharsets.UTF_8)
                FileChannel.open(
                    file.toPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND,
                ).use { channel ->
                    val buffer = ByteBuffer.wrap(bytes)
                    while (buffer.hasRemaining()) channel.write(buffer)
                    channel.force(true)
                }
                journalEventCount += eventCount
                pendingChanges.clear()
                namespaceDirty = false
                if (journalEventCount >= MAX_JOURNAL_EVENTS || file.length() >= MAX_JOURNAL_BYTES) {
                    compactSnapshot(parent)
                    journalEventCount = 0
                }
            }
            if (!marker.exists()) {
                Files.writeString(marker.toPath(), "initialized\n", StandardCharsets.UTF_8)
                FileChannel.open(marker.toPath(), StandardOpenOption.WRITE).use { it.force(true) }
            }
            return true
        } catch (e: Exception) {
            available = false
            LOG.warn("Could not write open Check Run registry at $file; Check Run writes are paused until storage is available: ${e.message}")
            return false
        }
    }

    private fun compactSnapshot(parent: File?) {
        val tmp = File(parent, "${file.name}.tmp")
        val contents = entries.values.sortedBy { it.openedAt }.joinToString("") { encode(it) } +
            "v3\tN\t$correlationNamespace\n"
        Files.writeString(tmp.toPath(), contents, StandardCharsets.UTF_8)
        FileChannel.open(tmp.toPath(), StandardOpenOption.WRITE).use { it.force(true) }
        try {
            Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private fun endsWithNewline(target: File): Boolean = RandomAccessFile(target, "r").use { raf ->
        raf.seek(raf.length() - 1)
        raf.read() == '\n'.code
    }

    private fun pruneTerminalEntries() {
        val terminal = terminalEntryIds.mapNotNull(entries::get)
        val cutoff = now() - terminalRetentionMs
        val expired = terminal.filter { it.openedAt < cutoff }
        val expiredIds = expired.mapTo(hashSetOf()) { it.promotionId }
        val retained = terminal.filterNot { it.promotionId in expiredIds }.sortedBy { it.openedAt }
        val excessCount = (retained.size - maxTerminalEntries.coerceAtLeast(0)).coerceAtLeast(0)
        val removeIds = expiredIds + retained.take(excessCount).map { it.promotionId }
        removeIds.forEach { promotionId ->
            entries.remove(promotionId)
            terminalEntryIds.remove(promotionId)
            pendingChanges[promotionId] = null
        }
    }

    private fun isTerminal(entry: Entry): Boolean =
        entry.createPhase == CheckRunCreatePhase.COMPLETED || entry.createPhase == CheckRunCreatePhase.ABANDONED

    private fun encode(entry: Entry): String = listOf(
        "v2",
        entry.promotionId.toString(),
        encodeField(entry.buildTypeExternalId),
        encodeField(entry.repoSlug.orEmpty()),
        encodeField(entry.checkName.orEmpty()),
        encodeField(entry.headSha),
        encodeField(entry.externalId.orEmpty()),
        entry.checkRunId?.toString().orEmpty(),
        entry.desiredStatus?.apiValue.orEmpty(),
        entry.confirmedStatus?.apiValue.orEmpty(),
        entry.createPhase.name,
        entry.pendingUpdate.toString(),
        entry.openedAt.toString(),
    ).joinToString("\t") + "\n"

    private fun encodeEvent(entry: Entry): String = "v3\tU\t" + encode(entry).substringAfter('\t')

    private fun parseEvent(line: String): RegistryEvent? {
        val fields = line.split('\t')
        if (fields.size < 3 || fields[0] != "v3") return null
        return when (fields[1]) {
            "N" -> {
                if (fields.size != 3) null
                else runCatching { UUID.fromString(fields[2]).toString() }.getOrNull()?.let(RegistryEvent::Namespace)
            }
            "D" -> {
                if (fields.size != 3) null
                else fields[2].toLongOrNull()?.let(RegistryEvent::Delete)
            }
            "U" -> {
                if (fields.size != 14) return null
                val entry = parse((listOf("v2") + fields.drop(2)).joinToString("\t")) ?: return null
                RegistryEvent.Upsert(entry)
            }
            else -> null
        }
    }

    private fun parse(line: String): Entry? {
        val fields = line.split('\t')
        if (fields.size == 4) {
            val id = fields[0].toLongOrNull() ?: return null
            val openedAt = fields[3].toLongOrNull() ?: return null
            if (fields[1].isBlank() || fields[2].isBlank()) return null
            return Entry(
                promotionId = id,
                buildTypeExternalId = fields[1],
                headSha = fields[2],
                openedAt = openedAt,
            )
        }
        if (fields.size != 13 || fields[0] != "v2") return null

        val id = fields[1].toLongOrNull() ?: return null
        val buildType = decodeField(fields[2]) ?: return null
        val repo = decodeField(fields[3]) ?: return null
        val checkName = decodeField(fields[4]) ?: return null
        val sha = decodeField(fields[5]) ?: return null
        val externalId = decodeField(fields[6]) ?: return null
        val runId = fields[7].takeIf { it.isNotEmpty() }?.toLongOrNull()
        if (fields[7].isNotEmpty() && runId == null) return null
        val desired = if (fields[8].isEmpty()) null else parseStatus(fields[8]) ?: return null
        val confirmed = if (fields[9].isEmpty()) null else parseStatus(fields[9]) ?: return null
        val phase = runCatching { CheckRunCreatePhase.valueOf(fields[10]) }.getOrNull() ?: return null
        val pendingUpdate = fields[11].toBooleanStrictOrNull() ?: return null
        val openedAt = fields[12].toLongOrNull() ?: return null
        if (buildType.isBlank() || sha.isBlank()) return null
        if (phase != CheckRunCreatePhase.LEGACY && (repo.isBlank() || checkName.isBlank())) return null
        if (phase != CheckRunCreatePhase.LEGACY && phase != CheckRunCreatePhase.ABANDONED &&
            (externalId.isBlank() || desired == null)
        ) return null
        if ((phase == CheckRunCreatePhase.ACTIVE || phase == CheckRunCreatePhase.COMPLETED) && runId == null) return null
        return Entry(
            promotionId = id,
            buildTypeExternalId = buildType,
            headSha = sha,
            openedAt = openedAt,
            repoSlug = repo.takeIf { it.isNotEmpty() },
            checkName = checkName.takeIf { it.isNotEmpty() },
            externalId = externalId.takeIf { it.isNotEmpty() },
            checkRunId = runId,
            desiredStatus = desired,
            confirmedStatus = confirmed,
            createPhase = phase,
            pendingUpdate = pendingUpdate,
        )
    }

    private fun parseStatus(value: String): CheckRunStatus? =
        CheckRunStatus.entries.firstOrNull { it.apiValue == value }

    private fun encodeField(value: String): String =
        Base64.getUrlEncoder().withoutPadding().encodeToString(value.toByteArray(StandardCharsets.UTF_8))

    private fun decodeField(value: String): String? = try {
        String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8)
    } catch (_: IllegalArgumentException) {
        null
    }

    private sealed interface RegistryEvent {
        data class Upsert(val entry: Entry) : RegistryEvent
        data class Delete(val promotionId: Long) : RegistryEvent
        data class Namespace(val value: String) : RegistryEvent
    }

    companion object {
        private val LOG = Logger.getInstance(OpenCheckRunRegistry::class.java.name)

        const val FILE_NAME: String = "open-check-runs.tsv"
        const val MAX_JOURNAL_EVENTS: Int = 512
        const val MAX_JOURNAL_BYTES: Long = 1_048_576L
        const val MAX_TERMINAL_ENTRIES: Int = 4_096
        const val TERMINAL_RETENTION_MS: Long = 90L * 24 * 60 * 60 * 1_000
    }
}

enum class CheckRunCreatePhase {
    RESERVED,
    POSTING,
    UNCERTAIN,
    RETRYABLE,
    REJECTED,
    ACTIVE,
    COMPLETED,
    ABANDONED,
    LEGACY,
}
