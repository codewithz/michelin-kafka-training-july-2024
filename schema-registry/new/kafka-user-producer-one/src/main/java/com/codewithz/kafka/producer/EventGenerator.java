package com.codewithz.kafka.producer;

import com.codewithz.kafka.producer.enums.Color;
import com.codewithz.kafka.producer.enums.DesignType;
import com.codewithz.kafka.producer.enums.ProductType;
import com.codewithz.kafka.producer.enums.UserId;
import com.codewithz.kafka.producer.model.Event;
import com.codewithz.kafka.producer.model.InternalProduct;
import com.codewithz.kafka.producer.model.InternalUser;
import com.github.javafaker.Faker;

public class EventGenerator {


    private Faker faker=new Faker();

    public Event generateEvent(){
        Event event=Event.builder()
                    .internalUser(generateRandomUser())
                    .internalProduct(generateRandomProduct())
                    .build();
        return event;
    }

    private InternalUser generateRandomUser(){
        InternalUser user=InternalUser.builder()
                        .userId(faker.options().option(UserId.class))
                        .username(faker.name().lastName())
                        .dateOfBirth(faker.date().birthday())
                        .build();
        return user;
    }

    private InternalProduct generateRandomProduct(){
        InternalProduct product=InternalProduct.builder()
                                .color(faker.options().option(Color.class))
                                .productType(faker.options().option(ProductType.class))
                                .designType(faker.options().option(DesignType.class))
                                   .build();

        return product;
    }
    }

