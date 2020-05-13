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
@Table(name = "dgf_groups")
public class DGFGroups {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    @Column(name = "id")
    private int key;

    @Column(name = "is_active")
    private Byte isActive;
    @Column(name = "last_modified_timestamp")
    private LocalDateTime lastModifiedTimestamp;
    @Column(name = "modified_by")
    private Integer modifiedBy;
    @Column(name = "name")
    private String baseRate;

    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn(name = "dgf_groups_id", referencedColumnName = "id")
    private Set<DGFSubGroupLevel1> children;


}
