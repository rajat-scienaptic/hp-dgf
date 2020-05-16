package com.hp.dgf.model;

import lombok.Data;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "dgf_logs")
public class DGFLogs {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    @Column(name = "id")
    private int id;

    @NotNull @NotBlank
    @Column(name = "operation")
    private String operation;
    @NotNull @NotBlank
    @Column(name = "message")
    private String message;
    @NotNull @NotBlank
    @Column(name = "create_time")
    private LocalDateTime createTime;
}
