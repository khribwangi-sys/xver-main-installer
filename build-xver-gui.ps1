$ErrorActionPreference='Stop'
$D=Join-Path $env:USERPROFILE 'xver-gui-build'
New-Item -Path $D -ItemType Directory -Force|Out-Null
Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/khribwangi-sys/xver-main-installer/main/main.py' -OutFile (Join-Path $D 'main.py')
Invoke-WebRequest -Uri 'https://s.imgz.io/2026/10/22/9e48c78d8db39b24af2432d5e061006ca34b1aeebe6366bf.jpg' -OutFile (Join-Path $D 'background.jpg')
$L=@'
import os,sys,runpy,traceback,threading,queue,io,contextlib,tkinter as tk
from tkinter import ttk,messagebox
from PIL import Image,ImageTk,ImageEnhance
BASE=getattr(sys,'_MEIPASS',os.path.dirname(os.path.abspath(__file__)))
APP=os.path.dirname(sys.executable) if getattr(sys,'frozen',False) else os.path.dirname(os.path.abspath(__file__))
class Writer:
    def __init__(self,q): self.q=q
    def write(self,s):
        if s.strip(): self.q.put(s.rstrip())
    def flush(self): pass
class App:
    def __init__(self):
        self.q=queue.Queue();self.running=False
        self.root=tk.Tk();self.root.title('บอทดักซอง');self.root.geometry('980x650');self.root.minsize(820,560);self.root.configure(bg='#0b1220')
        try:self.root.iconname('บอทดักซอง')
        except:pass
        img=Image.open(os.path.join(BASE,'background.jpg')).convert('RGB');img=img.resize((1200,800));img=ImageEnhance.Brightness(img).enhance(.30);self.bg=ImageTk.PhotoImage(img)
        self.canvas=tk.Canvas(self.root,highlightthickness=0,bg='#0b1220');self.canvas.pack(fill='both',expand=True);self.canvas.create_image(0,0,image=self.bg,anchor='nw')
        self.panel=tk.Frame(self.canvas,bg='#111a2b',highlightbackground='#33445f',highlightthickness=1);self.canvas.create_window(490,325,window=self.panel,width=720,height=555)
        tk.Label(self.panel,text='บอทดักซอง',font=('Segoe UI',30,'bold'),fg='#7dd3fc',bg='#111a2b').pack(pady=(26,2))
        tk.Label(self.panel,text='ระบบตรวจจับและแจ้งเตือนซองอั่งเปา',font=('Segoe UI',12),fg='#aab9cc',bg='#111a2b').pack(pady=(0,22))
        self.card=tk.Frame(self.panel,bg='#172337');self.card.pack(fill='x',padx=42,pady=2)
        tk.Label(self.card,text='Discord Bot Token',font=('Segoe UI',11,'bold'),fg='#e7eef8',bg='#172337',anchor='w').pack(fill='x',padx=18,pady=(16,6))
        self.token=tk.Entry(self.card,show='•',font=('Segoe UI',12),bg='#25354d',fg='white',insertbackground='white',relief='flat')
        self.token.pack(fill='x',padx=18,pady=(0,16),ipady=10)
        self.start=ttk.Button(self.panel,text='เริ่มทำงาน',command=self.start_bot);self.start.pack(pady=(18,8),ipadx=28,ipady=8)
        self.status=tk.Label(self.panel,text='● พร้อมเริ่มทำงาน',font=('Segoe UI',11,'bold'),fg='#7ee2a8',bg='#111a2b');self.status.pack(pady=(0,12))
        tk.Label(self.panel,text='สถานะการทำงาน',font=('Segoe UI',10,'bold'),fg='#b8c7d9',bg='#111a2b',anchor='w').pack(fill='x',padx=42)
        self.out=tk.Text(self.panel,height=11,bg='#0b1220',fg='#d6e2ee',insertbackground='white',relief='flat',font=('Consolas',9),padx=12,pady=10)
        self.out.pack(fill='both',expand=True,padx=42,pady=(6,16))
        self.root.after(150,self.poll);self.root.protocol('WM_DELETE_WINDOW',self.close)
    def log(self,s): self.out.insert('end',s+'\n');self.out.see('end')
    def start_bot(self):
        if self.running:return
        t=self.token.get().strip()
        if not t:return messagebox.showwarning('ยังไม่ได้ใส่ Token','กรุณาใส่ Discord Bot Token ก่อนเริ่มทำงาน')
        os.environ['DISCORD_BOT_TOKEN']=t;os.environ.setdefault('DB_PATH',os.path.join(APP,'bot_users.db'));self.running=True;self.start.config(state='disabled');self.token.config(state='disabled');self.status.config(text='● กำลังเชื่อมต่อ Discord...',fg='#f6c85f');self.log('กำลังตรวจสอบ Token และเชื่อมต่อ Discord...');threading.Thread(target=self.worker,daemon=True).start()
    def worker(self):
        try:
            w=Writer(self.q)
            with contextlib.redirect_stdout(w),contextlib.redirect_stderr(w):runpy.run_path(os.path.join(BASE,'main.py'),run_name='__main__')
        except BaseException as e:
            if type(e).__name__=='LoginFailure':self.q.put('TOKEN_ERROR')
            else:self.q.put('ERROR:'+traceback.format_exc())
        finally:self.q.put('__DONE__')
    def poll(self):
        try:
            while True:
                s=self.q.get_nowait()
                if s=='TOKEN_ERROR':self.log('Token ไม่ถูกต้องหรือหมดอายุ กรุณา Reset Token ใน Discord Developer Portal แล้วลองใหม่');self.status.config(text='● Token ไม่ถูกต้อง',fg='#ff7d8a')
                elif s.startswith('ERROR:'):self.log('เกิดข้อผิดพลาด:');self.log(s[6:]);self.status.config(text='● เกิดข้อผิดพลาด',fg='#ff7d8a')
                elif s=='__DONE__':self.running=False;self.start.config(state='normal');self.token.config(state='normal')
                else:self.log(s);self.status.config(text='● บอทกำลังทำงาน',fg='#7ee2a8')
        except queue.Empty:pass
        self.root.after(150,self.poll)
    def close(self):
        if self.running and not messagebox.askyesno('ปิดโปรแกรม','บอทยังทำงานอยู่ ต้องการปิดหรือไม่?'):return
        self.root.destroy()
App().root.mainloop()
'@
Set-Content (Join-Path $D 'gui_launcher.py') $L -Encoding UTF8
py -3 -m venv (Join-Path $D 'venv')
$V=Join-Path $D 'venv\Scripts\python.exe'
& $V -m pip install -q --upgrade pip pyinstaller discord.py telethon aiohttp Pillow zxing-cpp primp python-dotenv pynacl davey requests certifi truststore
Set-Location $D;$PI=Join-Path $D 'venv\Scripts\pyinstaller.exe'
& $PI --noconfirm --clean --onefile --windowed --name xver-main --add-data 'main.py;.' --add-data 'background.jpg;.' --collect-all aiohttp --collect-all discord --collect-all telethon --collect-all zxingcpp --collect-all primp --hidden-import PIL --hidden-import PIL.Image --hidden-import nacl gui_launcher.py
Copy-Item (Join-Path $D 'dist\xver-main.exe') (Join-Path $env:USERPROFILE 'Desktop\xver-main.exe') -Force
Write-Host "สร้างเสร็จ: $env:USERPROFILE\Desktop\xver-main.exe"
