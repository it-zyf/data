package model.herobean;


import Base.OtherBaseEntity;

/**
 * <p>
 * 任务日志
 * </p>
 *
 * @author twg
 * @since 2020-03-26
 */
public class ServerTask extends OtherBaseEntity {
    public String logName;
    public String taskId;
    public String taskType;
    public Integer taskStatus;
    public String spendSecond;

}
