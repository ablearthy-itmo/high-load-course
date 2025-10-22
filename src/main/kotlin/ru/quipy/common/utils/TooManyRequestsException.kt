package ru.quipy.common.utils

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException

class TooManyRequestsException(
    private val retryAfterSeconds: Long
) : ResponseStatusException(
    HttpStatus.TOO_MANY_REQUESTS,
    "Too many requests. Please try again later."
) {
    override fun getResponseHeaders(): HttpHeaders {
        val headers = HttpHeaders()
        headers.set("Retry-After", retryAfterSeconds.toString())
        return headers
    }
}
