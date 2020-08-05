package com.hp.dgf.model;

import com.fasterxml.jackson.annotation.JsonFormat;
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
@Table(name = "product_line")
public final class ProductLine {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private int id;

    @Column(name = "business_sub_category_id")
    private Integer businessSubCategoryId;
    @Column(name = "code")
    private String code;
    @Column(name = "is_active")
    private Byte isActive;
    @Column(name = "last_modified_timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm")
    private LocalDateTime lastModifiedTimestamp;
    @Column(name = "modified_by")
    private String modifiedBy;
    @Column(name = "base_rate")
    private BigDecimal baseRate;

    @OneToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "id", referencedColumnName = "product_line_id")
    private DGFRateEntry dgfRateEntry;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "color_code_id", referencedColumnName = "id")
    private ColorCode colorCodeSet;
}
