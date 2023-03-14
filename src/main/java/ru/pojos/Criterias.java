package ru.pojos;

import lombok.Data;

import java.io.Serializable;

@Data
public class Criterias implements Serializable {
    private Long id;
    private String parameter;
    private String operator;
    private String value;

}
