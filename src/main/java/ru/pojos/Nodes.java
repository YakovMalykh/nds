package ru.pojos;

import lombok.Data;

import java.io.Serializable;

@Data
public class Nodes implements Serializable {
    private String condition;
    private Long id;
}
