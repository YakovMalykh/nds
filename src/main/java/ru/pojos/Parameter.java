package ru.pojos;

import lombok.Data;

import java.io.Serializable;

@Data
public class Parameter implements Serializable {
    private String name;
    private String value;
    private String type;

}
