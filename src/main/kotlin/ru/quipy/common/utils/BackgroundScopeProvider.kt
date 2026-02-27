package ru.quipy.common.utils

import org.springframework.stereotype.Component
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*

@Component
class BackgroundScopeProvider {
    @OptIn(ExperimentalCoroutinesApi::class)
    val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    @OptIn(DelicateCoroutinesApi::class)
    val esScope = CoroutineScope(newFixedThreadPoolContext(16, "es-scope"))
    
    @PreDestroy
    fun cleanup() {
        scope.cancel()
        esScope.cancel()
    }
}
