package ru.quipy.common.utils

import org.springframework.stereotype.Component
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*

@Component
class BackgroundScopeProvider {
    val scope = CoroutineScope(Dispatchers.IO.limitedParallelism(32) + SupervisorJob())
    val esScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    
    @PreDestroy
    fun cleanup() {
        scope.cancel()
        esScope.cancel()
    }
}
