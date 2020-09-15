""" 抖音12.6版本滑动操作实现数据抓取"""
import time
import uiautomator2 as u2


class Douyin(object):
    # 在__init__方法里面连接设备
    def __init__(self, serial="SJQNW18A31000337"):
        # u2连接设备的四种方式
        # self.d = u2.connect_usb(serial=serial)
        # 注意这里connect_adb_wifi方式，端口不能忘记
        # self.d = u2.connect_adb_wifi("192.168.3.3:5555")
        # self.d = u2.connect_wifi("192.168.3.3")
        # 虚拟机用u2.connect()连接
        self.d = u2.connect()
        # 启动app
        self.start_app()
        # 获取屏幕分辨率
        self.size = self.get_windowsize()
        # 启动监视器
        self.handle_watcher()
        # 是用来获取一个初始时间
        self.t0 = time.perf_counter()

    def start_app(self):
        """启动app"""
        self.d.app_start(package_name="com.ss.android.ugc.aweme")

    def stop_app(self):
        """app退出逻辑"""
        # 先关闭监视器
        self.d.watcher.stop()
        self.d.app_stop("com.ss.android.ugc.aweme")
        self.d.app_clear("com.ss.android.ugc.aweme")

    def stop_time(self):
        """停止时间"""
        # 时间是秒
        if time.perf_counter() - self.t0 > 300:
            return True

    # u2监视器，用来处理不定时弹出窗口的事件
    def handle_watcher(self):
        """监视器"""
        #位置信息授权
        self.d.watcher.when('//*[@resource-id="com.android.permissioncontroller:id/permission_allow_foreground_only_button"]').click()
        #获取设备信息授权
        self.d.watcher.when('//*[@resource-id="com.android.permissioncontroller:id/permission_allow_button"]').click()
        # 个人信息保护指引
        self.d.watcher.when('//*[@resource-id="com.ss.android.ugc.aweme:id/akm"]').click()
        # 请求登录，返回退出
        self.d.watcher.when('//*[@resource-id="com.ss.android.ugc.aweme:id/ej2"]').click()
        # 直播监视器
        self.d.watcher.when('//*[@resource-id="com.ss.android.ugc.aweme:id/ade"]').click()
        # 提出直播后提示信息
        self.d.watcher.when('//*[@resource-id="com.ss.android.ugc.aweme:id/e53"]').click()
        # 广告链接
        self.d.watcher.when('//*[@resource-id="com.ss.android.ugc.aweme:id/arc"]/android.widget.LinearLayout[1]').click()
        # 监控器写好之后，一定要记得启动
        self.d.watcher.start(interval=1)

    def get_windowsize(self):
        """获取窗口大小"""
        return self.d.window_size()

    def swipe_douyin(self):
        """滑动抖音视频和点击视频发布者头像的操作"""
        # 来判断是否正常的进入到了视频页面
        # 网络情况不好也包含在内了
        # 进入正常的视频页面,开始滑动
        self.d.swipe_ext("top")
        if self.d(resourceId="com.ss.android.ugc.aweme:id/ge0", text="消息").exists(timeout=20):
            while True:
                if self.stop_time():
                    self.stop_app()
                    return
                # 查看是不是正常的发布者
                if self.d(resourceId="com.ss.android.ugc.aweme:id/bnp").exists :
                    time.sleep(1)
                    self.d.swipe_ext("left")
                    if self.d(resourceId="com.ss.android.ugc.aweme:id/l4").exists:
                        time.sleep(3)
                        self.d.swipe_ext("right")
                else:
                    if self.d(resourceId="com.ss.android.ugc.aweme:id/ge0", text="消息").exists:
                        time.sleep(1)
                        self.d.swipe_ext("top")
                if self.d(resourceId="com.ss.android.ugc.aweme:id/ge0", text="消息").exists and \
                        self.d(resourceId="com.ss.android.ugc.aweme:id/bnp").exists:
                    time.sleep(1)
                    # 进入正常的视频页面,开始滑动
                    self.d.swipe_ext("top")


if __name__ == '__main__':
    # 不断地循环，无需再一次次的启动
    # u2有时候启动进程时可能失败，使用try，except，再次尝试即可
    while True:
        try:
            d = Douyin()
            d.swipe_douyin()
            time.sleep(3)
        except:
            continue

