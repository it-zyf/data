package utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;

/**
 * spring WebApplicationContext工具类
 * @author
 *
 */
@Component
public class SpringContextUtil implements ApplicationContextAware {
	
	private static ApplicationContext wac;

	public static void setApplicationContextStaticlly(ApplicationContext webApplicationContext){
		wac = webApplicationContext;
	}
	
	public void setApplicationContext(ApplicationContext ac)
			throws BeansException {
		wac = (WebApplicationContext)ac;
	}
	
	public static ApplicationContext getApplicationContext(){
		return wac;
	}
	
	public static String getMessage(String key){
		return getApplicationContext().getMessage(key, null, null);
	}

	public static String getProperty(String key){
		Environment environment = wac.getEnvironment();
		return environment.getProperty(key);
	}

	public static <T> T getBean(Class<T> classes) {
		return wac.getBean(classes);
	}

	public static Object getBean(String name) {
		return wac.getBean(name);
	}
}
