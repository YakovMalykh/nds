package ru.pojos;

import lombok.Data;

import java.io.Serializable;

@Data
public class Variables implements Serializable {
    private String name;
    private String table;
    private String field;
    private String type;

}
