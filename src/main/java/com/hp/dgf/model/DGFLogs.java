package com.hp.dgf.model;

import lombok.Builder;
import lombok.Data;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

@Builder(toBuilder = true)
@Data
@Entity
@Table(name = "dgf_logs")
public class DGFLogs {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private int id;

    @NotNull @NotBlank
    @Column(name = "endpoint")
    private String endpoint;
    @NotNull @NotBlank
    @Column(name = "status")
    private String status;
    @NotNull @NotBlank
    @Column(name = "message")
    private String message;
    @NotNull
    @Column(name = "create_time")
    private LocalDateTime createTime;
}
