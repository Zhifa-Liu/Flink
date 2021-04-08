package cn.edu.neu.experiment.advertise_blacklist;

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
    private String clickUserId;
    private String advertiseId;
    private long clickCount;
}

