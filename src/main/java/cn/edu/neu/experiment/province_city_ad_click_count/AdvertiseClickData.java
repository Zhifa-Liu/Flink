package cn.edu.neu.experiment.province_city_ad_click_count;

import lombok.*;

/**
 * @author 32098
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class AdvertiseClickData {
    private String clickTime;
    private String clickUserProvince;
    private String clickUserCity;
    private String advertiseId;
    private int clickCount;
}
