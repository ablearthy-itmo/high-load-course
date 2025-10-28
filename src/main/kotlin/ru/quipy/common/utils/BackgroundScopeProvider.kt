package ru.quipy.common.utils

import org.springframework.stereotype.Component
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*

@Component
class BackgroundScopeProvider {
    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    
    @PreDestroy
    fun cleanup() {
        scope.cancel()
    }
}
