package com.hp.dgf.dto.request;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class DGFGroupDTO {
    private Integer id;
    private Integer businessCategoryId;
    private Integer dgfGroupsId;
    private Integer dgfSubGroupLevel1Id;
    private Integer dgfSubGroupLevel2Id;
    @NotNull
    private Integer modifiedBy;
    @NotNull @NotBlank
    private String name;
}
