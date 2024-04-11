package ru.quipy.common.utils

import java.util.concurrent.atomic.AtomicInteger

class TaskWindow(
   private val window: OngoingWindow,
) {
    var jobCount: AtomicInteger = AtomicInteger(0)

    fun release() {
        window.release()
        jobCount.decrementAndGet()
    }

    fun acquireWindow() {
        window.acquire()
    }
}