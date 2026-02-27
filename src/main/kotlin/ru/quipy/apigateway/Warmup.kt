package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.stereotype.Component
import ru.quipy.apigateway.APIController.OrderStatus
import ru.quipy.orders.repository.OrderRepository
import java.util.UUID

@Component
class Warmup : ApplicationListener<ApplicationReadyEvent> {
    val logger: Logger = LoggerFactory.getLogger(Warmup::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var apiController: APIController

    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        logger.info("Warmup started")

        orderRepository.findById(UUID.randomUUID())

        for (i in 0..10000) {
            val o = APIController.Order(
                UUID.randomUUID(),
                UUID.randomUUID(),
                System.currentTimeMillis(),
                OrderStatus.COLLECTING,
                10
            )

            orderRepository.save(o)
            orderRepository.findById(o.id)
            orderRepository.delete(o.id)
        }


        for (i in 0..1000) {
            val o = apiController.createOrder(UUID.randomUUID(), 10)
            apiController.payOrder(o.id, System.currentTimeMillis() + 10_000)
            orderRepository.delete(o.id)
        }

        logger.info("Warmup finished!")
    }
}