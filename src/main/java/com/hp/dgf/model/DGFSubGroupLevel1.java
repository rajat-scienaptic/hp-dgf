package com.hp.dgf.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Where;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.Set;


@Builder(toBuilder = true)
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
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm")
    private LocalDateTime lastModifiedTimestamp;
    @Column(name = "modified_by")
    private String modifiedBy;
    @Column(name = "name")
    private String baseRate;

    @OneToMany(fetch = FetchType.EAGER)
    @Where(clause = "is_active = 1")
    @JoinColumn(name = "dgf_sub_group_level_1_id", referencedColumnName = "id")
    private Set<DGFSubGroupLevel2> children;

}
