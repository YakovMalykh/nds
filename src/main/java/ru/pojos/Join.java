package ru.pojos;

import lombok.Data;

import java.io.Serializable;
@Data
public class Join implements Serializable {
    private String tableLeft;
    private String entityLeft;
    private String tableRight;
    private String entityRight;
    private String type;

}
