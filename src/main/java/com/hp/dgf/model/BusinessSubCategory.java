package com.hp.dgf.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "business_sub_category")
public final class BusinessSubCategory {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name = "id")
    private int id;

    @Column(name = "business_category_id")
    private int businessCategoryId;
    @Column(name = "description")
    private String description;
    @Column(name = "is_active")
    private Byte isActive;
    @Column(name = "last_modified_timestamp")
    private LocalDateTime lastModifiedTimestamp;
    @Column(name = "modified_by")
    private Integer modifiedBy;
    @Column(name = "name")
    private String name;

    @OneToMany(fetch = FetchType.LAZY)
    @JoinColumn(name = "business_sub_category_id", referencedColumnName = "id")
    private Set<ProductLine> columns;

}
