package cn.edu.neu.experiment;

import lombok.*;

/**
 * @author 32098
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class AdvertiseClickBean {
    private String advertiseId;
    private Long clickTime;
    private String clickUserId;
    private String clickUserProvince;
    private String clickUserCity;
}
