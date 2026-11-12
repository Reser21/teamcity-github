package io.github.dlachouette.teamcity.github.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpServer
import io.github.dlachouette.teamcity.github.testsupport.LoggerBootstrap
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class GitHubClientCheckRunsTest {

    init { LoggerBootstrap.install() }

    private val repo = RepoCoords("owner", "repo")
    private val identity = CheckRunLookupIdentity("sha123", "TeamCity / Build")

    @Test
    fun `create parses the numeric id and sends external id without retrying an ambiguous post`(){
        val client = StubClient().apply {
            respond = { HttpResponse(201, """{"id":12345678901,"external_id":"tcgh:1:hash"}""", emptyMap()) }
        }
        val request = request(CheckRunStatus.QUEUED).copy(externalId = "tcgh:1:hash")

        assertEquals(CheckRunCreateResult.Created(12345678901L), client.createCheckRun("token", repo, request, "https://api.example"))

        val call = client.calls.single()
        assertEquals("POST", call.method)
        assertEquals("https://api.example/repos/owner/repo/check-runs", call.url)
        assertFalse(call.retryAmbiguousFailures)
        val body = ObjectMapper().readTree(call.body)
        assertEquals("tcgh:1:hash", body.path("external_id").asText())
        assertEquals("queued", body.path("status").asText())
    }

    @Test
    fun `create treats missing id and server errors as uncertain`(){
        val malformed = StubClient().apply { respond = { HttpResponse(201, "{}", emptyMap()) } }
        val serverError = StubClient().apply { respond = { HttpResponse(500, "{}", emptyMap()) } }
        val request = request(CheckRunStatus.QUEUED).copy(externalId = "tcgh:2:hash")

        assertEquals(CheckRunCreateResult.Uncertain, malformed.createCheckRun("token", repo, request))
        assertEquals(CheckRunCreateResult.Uncertain, serverError.createCheckRun("token", repo, request))
        assertEquals(1, serverError.calls.size, "an uncertain POST is never blindly retried")
    }

    @Test
    fun `uncorrelated creates also disable retries after ambiguous responses`(){
        val client = StubClient().apply { respond = { null } }

        assertEquals(CheckRunCreateResult.Uncertain, client.createCheckRun("token", repo, request(CheckRunStatus.QUEUED)))
        assertFalse(client.calls.single().retryAmbiguousFailures)
    }

    @Test
    fun `http 429 is classified as a retryable rate limit`(){
        val client = StubClient().apply { respond = { HttpResponse(429, "{}", emptyMap()) } }

        assertEquals(CheckRunCreateResult.Rejected(429, retryable = true), client.createCheckRun("token", repo, request(CheckRunStatus.QUEUED)))
    }

    @Test
    fun `update patches the exact run id and omits create identity fields`(){
        val client = StubClient().apply { respond = { HttpResponse(200, "{}", emptyMap()) } }
        val request = request(CheckRunStatus.COMPLETED).copy(externalId = "tcgh:3:hash")

        assertEquals(CheckRunUpdateResult.Accepted, client.updateCheckRun("token", repo, 987654321L, request, "https://api.example"))

        val call = client.calls.single()
        assertEquals("PATCH", call.method)
        assertEquals("https://api.example/repos/owner/repo/check-runs/987654321", call.url)
        assertFalse(call.retryAmbiguousFailures)
        val body = ObjectMapper().readTree(call.body)
        assertTrue(body.path("name").isMissingNode)
        assertTrue(body.path("head_sha").isMissingNode)
        assertTrue(body.path("external_id").isMissingNode)
        assertEquals("completed", body.path("status").asText())
    }

    @Test
    fun `real JDK transport sends create then patch and reads success and error bodies`() {
        val received = CopyOnWriteArrayList<RecordedRequest>()
        val retryAttempts = AtomicInteger()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { exchange ->
            val body = exchange.requestBody.use { it.readBytes().toString(StandardCharsets.UTF_8) }
            received += RecordedRequest(
                method = exchange.requestMethod,
                path = exchange.requestURI.path,
                body = body,
                authorization = exchange.requestHeaders.getFirst("Authorization"),
                accept = exchange.requestHeaders.getFirst("Accept"),
                apiVersion = exchange.requestHeaders.getFirst("X-GitHub-Api-Version"),
                contentType = exchange.requestHeaders.getFirst("Content-Type"),
            )
            val retryAttempt = if (exchange.requestURI.path == "/retry") retryAttempts.incrementAndGet() else 0
            val (status, responseBody) = when {
                exchange.requestURI.path == "/error" -> 422 to """{"message":"validation failed"}"""
                exchange.requestMethod == "POST" -> 201 to """{"id":456,"status":"queued"}"""
                exchange.requestMethod == "PATCH" -> 200 to """{"id":456,"status":"completed"}"""
                exchange.requestURI.path == "/retry" && retryAttempt == 1 -> 429 to """{"message":"rate limited"}"""
                exchange.requestURI.path == "/retry" -> 200 to """{"ok":true}"""
                else -> 404 to """{"message":"unexpected request"}"""
            }
            val bytes = responseBody.toByteArray(StandardCharsets.UTF_8)
            exchange.responseHeaders.set("Content-Type", "application/json")
            if (exchange.requestURI.path == "/retry" && retryAttempt == 1) {
                exchange.responseHeaders.set("Retry-After", "0")
            }
            exchange.sendResponseHeaders(status, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        server.start()

        try {
            val base = "http://127.0.0.1:${server.address.port}"
            val client = RealRequestClient().apply {
                apiVersion = "2022-11-28"
                maxAttempts = 2
                baseDelayMs = 0
            }
            val create = request(CheckRunStatus.QUEUED).copy(externalId = "tcgh:test:identity")
            val update = request(CheckRunStatus.COMPLETED).copy(
                externalId = "tcgh:test:identity",
                conclusion = CheckRunConclusion.CANCELLED,
            )

            assertEquals(CheckRunCreateResult.Created(456), client.createCheckRun("test-token", repo, create, base))
            assertEquals(CheckRunUpdateResult.Accepted, client.updateCheckRun("test-token", repo, 456, update, base))
            val error = client.sendRaw("PATCH", "$base/error", "test-token", "{}")
            val retried = client.sendRaw("GET", "$base/retry", "test-token")

            assertEquals(422, error?.code)
            assertEquals("""{"message":"validation failed"}""", error?.body)
            assertEquals(200, retried?.code)
            assertEquals("""{"ok":true}""", retried?.body)
            assertEquals(listOf("POST", "PATCH", "PATCH", "GET", "GET"), received.map { it.method })
            assertEquals(2, received.count { it.path == "/retry" }, "an explicit rate-limit response retries")
            assertEquals("/error", received[2].path)
            assertEquals("{}", received[2].body)

            val post = received[0]
            assertEquals("/repos/owner/repo/check-runs", post.path)
            assertEquals("Bearer test-token", post.authorization)
            assertEquals(GitHubClient.ACCEPT_HEADER, post.accept)
            assertEquals("2022-11-28", post.apiVersion)
            assertEquals("application/json", post.contentType)
            val createBody = ObjectMapper().readTree(post.body)
            assertEquals("TeamCity / Build", createBody.path("name").asText())
            assertEquals("sha123", createBody.path("head_sha").asText())
            assertEquals("tcgh:test:identity", createBody.path("external_id").asText())

            val patch = received[1]
            assertEquals("/repos/owner/repo/check-runs/456", patch.path)
            assertEquals("Bearer test-token", patch.authorization)
            assertEquals("2022-11-28", patch.apiVersion)
            assertEquals("application/json", patch.contentType)
            val patchBody = ObjectMapper().readTree(patch.body)
            assertTrue(patchBody.path("name").isMissingNode)
            assertTrue(patchBody.path("head_sha").isMissingNode)
            assertTrue(patchBody.path("external_id").isMissingNode)
            assertEquals("completed", patchBody.path("status").asText())
            assertEquals("cancelled", patchBody.path("conclusion").asText())
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `lookup uses all check runs and exact external id name and sha`(){
        val client = StubClient().apply {
            respond = {
                HttpResponse(
                    200,
                    """{"total_count":2,"check_runs":[
                      {"id":1,"external_id":null,"name":"Unrelated check","head_sha":"sha123","status":"queued"},
                      {"id":2,"external_id":"tcgh:4:hash","name":"TeamCity / Build","head_sha":"sha123","status":"in_progress"}
                    ]}""",
                    emptyMap(),
                )
            }
        }

        assertEquals(
            CheckRunLookup.Found(CheckRunSummary(2, "tcgh:4:hash", "TeamCity / Build", "sha123", CheckRunStatus.IN_PROGRESS)),
            client.findCheckRunByExternalId("token", repo, identity.headSha, identity.checkName, "tcgh:4:hash", "https://api.example"),
        )
        val call = client.calls.single()
        assertTrue(call.url.contains("/commits/sha123/check-runs?"))
        assertTrue(call.url.contains("check_name=TeamCity+%2F+Build"))
        assertTrue(call.url.contains("filter=all&per_page=100&page=1"))
    }

    @Test
    fun `lookup refuses ambiguous matches and malformed responses`(){
        val duplicate = """{"id":9,"external_id":"same","name":"TeamCity / Build","head_sha":"sha123","status":"queued"}"""
        val ambiguous = StubClient().apply {
            respond = { HttpResponse(200, """{"total_count":2,"check_runs":[$duplicate,$duplicate]}""", emptyMap()) }
        }
        val malformed = StubClient().apply { respond = { HttpResponse(200, "not json", emptyMap()) } }

        assertEquals(CheckRunLookup.Ambiguous, ambiguous.findCheckRunByExternalId("token", repo, "sha123", "TeamCity / Build", "same"))
        assertEquals(CheckRunLookup.Unavailable, malformed.findCheckRunByExternalId("token", repo, "sha123", "TeamCity / Build", "same"))
    }

    private fun request(status: CheckRunStatus) = CheckRunRequest(
        name = "TeamCity / Build",
        headSha = "sha123",
        status = status,
        conclusion = if (status == CheckRunStatus.COMPLETED) CheckRunConclusion.SUCCESS else null,
        outputTitle = "Build",
        outputSummary = "summary",
    )

    private data class CheckRunLookupIdentity(val headSha: String, val checkName: String)

    private data class RecordedRequest(
        val method: String,
        val path: String,
        val body: String,
        val authorization: String?,
        val accept: String?,
        val apiVersion: String?,
        val contentType: String?,
    )

    private class RealRequestClient : GitHubClient() {
        fun sendRaw(method: String, url: String, token: String?, body: String? = null) = request(method, url, token, body)
    }

    private class StubClient : GitHubClient() {
        val calls = mutableListOf<Call>()
        var respond: (Call) -> HttpResponse? = { HttpResponse(404, "{}", emptyMap()) }

        override fun request(
            method: String,
            urlSpec: String,
            token: String?,
            jsonBody: String?,
            retryAmbiguousFailures: Boolean,
        ): HttpResponse? {
            val call = Call(method, urlSpec, jsonBody, retryAmbiguousFailures)
            calls += call
            return respond(call)
        }

        data class Call(
            val method: String,
            val url: String,
            val body: String?,
            val retryAmbiguousFailures: Boolean,
        )
    }
}
