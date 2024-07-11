package com.codewithz.kafka.producer.model;

import java.util.Date;

import org.checkerframework.checker.units.qual.Angle;

import com.codewithz.kafka.producer.enums.UserId;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class InternalUser {
    private UserId userId;

    private String username;

    private Date dateOfBirth;
}
