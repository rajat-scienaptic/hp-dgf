package com.hp.dgf.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

@Builder(toBuilder = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "product_line")
public class ProductLine {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    @Column(name = "id")
    private int key;

    @Column(name = "business_sub_category_id")
    private Integer businessSubCategoryId;
    @Column(name = "code")
    private String code;
    @Column(name = "is_active")
    private Byte isActive;
    @Column(name = "last_modified_timestamp")
    private LocalDateTime lastModifiedTimestamp;
    @Column(name = "modified_by")
    private Integer modifiedBy;
    @Column(name = "dgf_sub_group_level_2_id")
    private Integer dgfSubGroupLevel2Id;
    @Column(name = "dgf_sub_group_level_3_id")
    private Integer dgfSubGroupLevel3Id;

    @OneToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "id", referencedColumnName = "product_line_id")
    private DGFRateEntry dgfRateEntry;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "color_code_id", referencedColumnName = "id")
    private ColorCode colorCodeSet;
}
