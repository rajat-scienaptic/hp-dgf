package com.hp.dgf.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Where;

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
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm")
    private LocalDateTime lastModifiedTimestamp;
    @Column(name = "modified_by")
    private Integer modifiedBy;
    @Column(name = "name")
    private String name;

    @OneToMany(fetch = FetchType.LAZY)
    @Where(clause = "is_active = 1")
    @JoinColumn(name = "business_sub_category_id", referencedColumnName = "id")
    private Set<ProductLine> columns;

}
