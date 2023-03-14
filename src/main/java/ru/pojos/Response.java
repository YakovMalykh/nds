package ru.pojos;

import lombok.Data;

import java.io.Serializable;
@Data
public class Response implements Serializable {
    private Long fid;
    private Long year;
    private Long quarter;
}
