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
@Table(name = "dgf_sub_group_level_1")
public final class DGFSubGroupLevel1 {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name = "id")
    private int id;

    @Column(name = "dgf_groups_id")
    private Integer dgfGroupsId;
    @Column(name = "is_active")
    private Byte isActive;
    @Column(name = "last_modified_by")
    private LocalDateTime lastModifiedBy;
    @Column(name = "modified_by")
    private Integer modifiedBy;
    @Column(name = "name")
    private String baseRate;

    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn(name = "dgf_sub_group_level_1_id", referencedColumnName = "id")
    private Set<DGFSubGroupLevel2> children;

}
