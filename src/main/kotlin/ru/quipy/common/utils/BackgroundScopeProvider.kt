package ru.quipy.common.utils

import org.springframework.stereotype.Component
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*

@Component
class BackgroundScopeProvider {
    @OptIn(ExperimentalCoroutinesApi::class)
    val scope = CoroutineScope(Dispatchers.IO.limitedParallelism(8) + SupervisorJob())
    @OptIn(ExperimentalCoroutinesApi::class)
    val esScope = CoroutineScope(Dispatchers.IO.limitedParallelism(8) + SupervisorJob())
    
    @PreDestroy
    fun cleanup() {
        scope.cancel()
        esScope.cancel()
    }
}
