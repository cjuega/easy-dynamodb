package com.cjuega.easydynamodb

import net.datafaker.Faker
import java.util.*

object ObjectMother {
    private val faker = Faker()

    fun coin() = faker.bool().bool()

    fun size(max: Int? = Int.MAX_VALUE) = int(0, max)

    fun int(min: Int? = Int.MIN_VALUE, max: Int? = Int.MAX_VALUE) = faker.number().numberBetween(min!!, max!!)

    fun uuid() = UUID.randomUUID().toString()
}