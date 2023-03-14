package ru.pojos;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class CalcSequence implements Serializable {
    private Long id;
    private List<Long> criterias;
    private String operator;
    private List<Nodes> nodes;
    private String loss;
}
