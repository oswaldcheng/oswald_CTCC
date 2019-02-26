package com.oswald.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @ClassName QueryInfo
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/26
 * @Version V1.0
 **/
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class QueryInfo {
    private String telephone;
    private String year;
    private String month;
    private String day;

}
