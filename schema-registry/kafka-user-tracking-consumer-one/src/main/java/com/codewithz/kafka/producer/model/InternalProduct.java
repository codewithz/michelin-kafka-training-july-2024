package com.codewithz.kafka.producer.model;


import com.codewithz.kafka.producer.enums.Color;
import com.codewithz.kafka.producer.enums.DesignType;
import com.codewithz.kafka.producer.enums.ProductType;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class InternalProduct {

    private Color color;

    private ProductType productType;

    private DesignType designType;

}
