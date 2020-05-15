package utils;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by jason on 2017/7/11.
 */
public class R<T> {

    private int code = -1;
    private String msg = "";
    private T data;

    /**
     * 必须有默认构造函数
     */
    public R() {

    }

    public R(int code, String msg, T data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public int getCode() {
        return code;
    }

    public R setCode(int code) {
        this.code = code;
        return this;
    }

    public boolean ok() {
        return 0 == code;
    }

    public T getData() {
        return data;
    }

    public R setData(T data) {
        this.data = data;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public R setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    @Override
    public String toString() {
        return "R{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                ", data=" + data +
                '}';
    }

    public static <T> R<T> handle() {
        return R.ok("操作成功");
    }

    public static <T> R<T> update(String... msg) {
        final String join = StringUtils.join(msg, ",");
        return R.ok("更新成功" + (StringUtils.isNotEmpty(join) ? ":" + join : ""));
    }

    public static <T> R<T> create() {
        return R.ok("创建成功");
    }

    public static <T> R<T> enable(boolean enable) {
        return R.ok(enable ? "已启用" : "已禁用");
    }

    public static <T> R<T> delete() {
        return R.ok("删除成功");
    }

    public static <T> R<T> clear() {
        return R.ok("清理成功");
    }

    public static <T> R<T> ok(String msg) {
        return new R<>(0, msg, null);
    }

    public static <T> R<T> ok(String msg, T data) {
        return new R<>(0, msg, data);
    }

    public static <T> R<List<T>> list(List<T> data) {
        return new R<>(0, "列表获取成功", data);
    }

    public static <T> R<T> entity(T data) {
        return new R<>(0, "成功", data);
    }


    public static <T> R<T> error(String msg) {
        return new R<>(-1, msg, null);
    }

    public static <T> R<T> error(int code, String msg) {
        return new R<>(code, msg, null);
    }

    public static <T> R<T> error(int code, String msg, T data) {
        return new R<>(code, msg, data);
    }

    public static <T> R<T> result(boolean ok) {
        return ok ? ok("操作成功") : error("操作失败");
    }

    public static <T> R<T> result(int ok) {
        return ok > 0 ? ok("操作成功") : error("操作失败");
    }


}
