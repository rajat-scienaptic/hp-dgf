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
@Table(name = "dgf_sub_group_level_2")
public class DGFSubGroupLevel2 {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    @Column(name = "id")
    private int key;

    @Column(name = "dgf_sub_group_level_1_id")
    private Integer dgfSubGroupLevel1Id;
    @Column(name = "is_active")
    private Byte isActive;
    @Column(name = "last_modified_by")
    private LocalDateTime lastModifiedBy;
    @Column(name = "modified_by")
    private Integer modifiedBy;
    @Column(name = "name")
    private String baseRate;

    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn(name = "dgf_sub_group_level_2_id", referencedColumnName = "id")
    private Set<DGFSubGroupLevel3> children;

    @OneToMany(fetch = FetchType.LAZY)
    @JoinColumn(name = "dgf_sub_group_level_2_id", referencedColumnName = "id")
    private Set<ProductLine> columns;

}
