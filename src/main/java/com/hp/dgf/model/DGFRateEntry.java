package com.hp.dgf.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Builder(toBuilder = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "dgf_rate_entry")
public class DGFRateEntry {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name = "id")
    private int id;

    @Column(name = "attachment_id")
    private Integer attachmentId;
    @Column(name = "created_by")
    private String createdBy;
    @Column(name = "created_on")
    private LocalDateTime createdOn;
    @Column(name = "dgf_rate")
    private BigDecimal dgfRate;
    @Column(name = "dgf_sub_group_level_2_id")
    private Integer dgfSubGroupLevel2Id;
    @Column(name = "dgf_sub_group_level_3_id")
    private Integer dgfSubGroupLevel3Id;
    @Column(name = "note")
    private String note;
    @Column(name = "product_line_id")
    private Integer productLineId;
}
