package com.hp.dgf.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "dgf_rate_change_log")
public final class DGFRateChangeLog {
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
    private String dgfRate;
    @Column(name = "dgf_rate_entry_id")
    private Integer dgfRateEntryId;
    @Column(name = "note")
    private String note;
}
