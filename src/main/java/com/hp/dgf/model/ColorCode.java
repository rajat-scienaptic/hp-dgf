package com.hp.dgf.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Builder(toBuilder = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "color_code")
public class ColorCode {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    @Column(name = "id")
    private int key;

    @Column(name = "code")
    private String code;
    @Column(name = "fy_quarter")
    private String fyQuarter;
    @Column(name = "fy_year")
    private String fyYear;
    @Column(name = "name")
    private String name;
}
