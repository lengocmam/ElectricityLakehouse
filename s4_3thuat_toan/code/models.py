"""S4D (ban cheo cua S4) va mo hinh du bao chuoi thoi gian.

S4D thay ma tran A kieu NPLR bang ma tran cheo phuc (Gu et al., 2022, arXiv 2206.11893),
don gian hon nhieu nhung giu nguyen y tuong: hai goc nhin (tich chap / de quy) cua cung mot SSM.

Chay  python models.py  de kiem tra: ket qua tich chap == ket qua de quy (giong phep tinh tay).
"""
import math
import torch
import torch.nn as nn
import torch.nn.functional as F


def _anchor(t, H, kind):
    """Moc neo trong khong gian da chuan hoa cua cua so: 'last' = gia tri hom nay, 'week' = cung thu tuan truoc
    (giong seasonal naive 7 ngay). Mo hinh chi hoc PHAN LECH so voi moc neo."""
    if kind == "last":
        return t[:, -1:].expand(-1, H)
    if kind == "week":
        L = t.shape[1]
        return t[:, [L - 1 + h - 7 * math.ceil(h / 7) for h in range(1, H + 1)]]
    return 0.0


class S4DKernel(nn.Module):
    """Tinh kernel tich chap K (H, L) tu cac tham so SSM cheo phuc (ZOH)."""

    def __init__(self, d_model, d_state=64, dt_min=0.01, dt_max=0.5):
        super().__init__()
        H, N = d_model, d_state // 2
        log_dt = torch.rand(H) * (math.log(dt_max) - math.log(dt_min)) + math.log(dt_min)
        C = torch.randn(H, N, dtype=torch.cfloat)
        self.C = nn.Parameter(torch.view_as_real(C))
        self.log_dt = nn.Parameter(log_dt)
        self.log_A_real = nn.Parameter(torch.log(0.5 * torch.ones(H, N)))
        self.A_imag = nn.Parameter(math.pi * torch.arange(N).float().repeat(H, 1))  # S4D-Lin
        for p in (self.log_dt, self.log_A_real, self.A_imag):
            p._no_wd = True  # tham so SSM: khong weight decay

    def discretize(self):
        dt = torch.exp(self.log_dt)                                   # (H)
        A = -torch.exp(self.log_A_real) + 1j * self.A_imag            # (H, N)
        dtA = A * dt.unsqueeze(-1)
        A_bar = torch.exp(dtA)
        C = torch.view_as_complex(self.C) * (A_bar - 1.0) / A         # da gom B_bar
        return dtA, A_bar, C

    def forward(self, L):
        dtA, _, C = self.discretize()
        K = dtA.unsqueeze(-1) * torch.arange(L, device=dtA.device)    # (H, N, L)
        return 2 * torch.einsum("hn,hnl->hl", C, torch.exp(K)).real   # (H, L)


class S4D(nn.Module):
    def __init__(self, d_model, d_state=64, dropout=0.0, dt_min=0.01, dt_max=0.5):
        super().__init__()
        self.D = nn.Parameter(torch.randn(d_model))
        self.kernel = S4DKernel(d_model, d_state, dt_min, dt_max)
        self.act = nn.GELU()
        self.drop = nn.Dropout(dropout)
        self.out = nn.Sequential(nn.Conv1d(d_model, 2 * d_model, 1), nn.GLU(dim=-2))

    def forward(self, u):                       # u: (B, H, L)
        L = u.size(-1)
        k = self.kernel(L)
        k_f = torch.fft.rfft(k, n=2 * L)        # chap bang FFT (dem 0 de tranh chap vong)
        u_f = torch.fft.rfft(u, n=2 * L)
        y = torch.fft.irfft(u_f * k_f, n=2 * L)[..., :L]
        y = y + u * self.D.unsqueeze(-1)
        return self.out(self.drop(self.act(y)))


class S4Block(nn.Module):
    def __init__(self, d_model, d_state, dropout, dt_min, dt_max):
        super().__init__()
        self.norm = nn.LayerNorm(d_model)
        self.s4 = S4D(d_model, d_state, dropout, dt_min, dt_max)
        self.drop = nn.Dropout(dropout)

    def forward(self, x):                       # x: (B, L, H)
        z = self.norm(x).transpose(1, 2)
        z = self.s4(z).transpose(1, 2)
        return x + self.drop(z)


class S4Forecaster(nn.Module):
    """x (B, L, F) voi kenh 0 la target -> du bao H buoc ke tiep cua target.

    Chuan hoa theo tung cua so (kieu RevIN) chi cho kenh target de giam anh huong xu huong/lech phan phoi.
    """

    def __init__(self, n_feat, horizon, d_model=64, n_layers=3, d_state=64,
                 dropout=0.1, dt_min=0.01, dt_max=0.5, n_future=0, anchor="none"):
        super().__init__()
        self.n_future, self.anchor, self.horizon = n_future, anchor, horizon
        self.enc = nn.Linear(n_feat, d_model)
        self.blocks = nn.ModuleList(
            [S4Block(d_model, d_state, dropout, dt_min, dt_max) for _ in range(n_layers)])
        self.norm = nn.LayerNorm(d_model)
        if n_future:   # dau ra nhan them dac trung biet truoc cua H ngay can du bao (lich, ngay le)
            self.head = nn.Sequential(nn.Linear(d_model + horizon * n_future, d_model), nn.GELU(),
                                      nn.Dropout(dropout), nn.Linear(d_model, horizon))
        else:
            self.head = nn.Linear(d_model, horizon)

    def forward(self, x, fut=None):
        t = x[..., :1]
        m = t.mean(1, keepdim=True)
        s = t.std(1, keepdim=True) + 1e-3
        x = torch.cat([(t - m) / s, x[..., 1:]], dim=-1)
        h = self.enc(x)
        for b in self.blocks:
            h = b(h)
        last = self.norm(h)[:, -1]
        if self.n_future:
            last = torch.cat([last, fut.flatten(1)], dim=1)
        out = self.head(last) + _anchor(t.squeeze(-1), self.horizon, self.anchor)   # (B, H)
        return out * s.squeeze(-1) + m.squeeze(-1)


def _norm0(x):
    """Chuan hoa kieu RevIN cho kenh target (kenh 0) theo tung cua so."""
    t = x[..., 0]
    m = t.mean(1, keepdim=True)
    s = t.std(1, keepdim=True) + 1e-3
    return (t - m) / s, m, s


class DLinear(nn.Module):
    """DLinear (Zeng et al., AAAI 2023): tach xu huong bang trung binh truot + hai lop tuyen tinh.
    Ban don bien: chi dung kenh target (dung nhu bai goc)."""

    def __init__(self, lookback, horizon, kernel=25):
        super().__init__()
        self.kernel = kernel if kernel % 2 == 1 else kernel + 1
        self.lin_s = nn.Linear(lookback, horizon)
        self.lin_t = nn.Linear(lookback, horizon)
        for lin in (self.lin_s, self.lin_t):
            nn.init.constant_(lin.weight, 1.0 / lookback)
            nn.init.zeros_(lin.bias)

    def forward(self, x):
        t, m, s = _norm0(x)
        pad = (self.kernel - 1) // 2
        tp = torch.cat([t[:, :1].repeat(1, pad), t, t[:, -1:].repeat(1, pad)], dim=1)
        trend = F.avg_pool1d(tp.unsqueeze(1), self.kernel, stride=1).squeeze(1)
        out = self.lin_s(t - trend) + self.lin_t(trend)
        return out * s + m


class PatchTST(nn.Module):
    """PatchTST don bien (Nie et al., ICLR 2023): cat chuoi thanh cac patch, Transformer encoder, dau ra tuyen tinh.
    Chi dung kenh target (channel-independent, mot kenh)."""

    def __init__(self, lookback, horizon, patch_len=8, stride=4, d_model=64, n_heads=4,
                 n_layers=2, d_ff=128, dropout=0.1):
        super().__init__()
        self.patch_len, self.stride = patch_len, stride
        n_patches = (lookback - patch_len) // stride + 1
        self.embed = nn.Linear(patch_len, d_model)
        self.pos = nn.Parameter(torch.zeros(1, n_patches, d_model))
        nn.init.trunc_normal_(self.pos, std=0.02)
        layer = nn.TransformerEncoderLayer(d_model, n_heads, d_ff, dropout, activation="gelu",
                                           batch_first=True, norm_first=True)
        self.enc = nn.TransformerEncoder(layer, n_layers, enable_nested_tensor=False)
        self.norm = nn.LayerNorm(d_model)
        self.head = nn.Sequential(nn.Flatten(), nn.Dropout(dropout), nn.Linear(n_patches * d_model, horizon))

    def forward(self, x):
        t, m, s = _norm0(x)
        p = t.unfold(1, self.patch_len, self.stride)           # (B, n_patches, patch_len)
        h = self.enc(self.embed(p) + self.pos)
        return self.head(self.norm(h)) * s + m


class _XLayer(nn.Module):
    """Mot lop TimeXer: self-attention tren (patch noi sinh + token toan cuc), cross-attention tu token toan cuc
    sang cac token bien ngoai sinh, roi FFN."""

    def __init__(self, d_model, n_heads, d_ff, dropout, has_ex):
        super().__init__()
        self.sa = nn.MultiheadAttention(d_model, n_heads, dropout=dropout, batch_first=True)
        self.ca = nn.MultiheadAttention(d_model, n_heads, dropout=dropout, batch_first=True) if has_ex else None
        self.n1, self.n2, self.n3 = nn.LayerNorm(d_model), nn.LayerNorm(d_model), nn.LayerNorm(d_model)
        self.ff = nn.Sequential(nn.Linear(d_model, d_ff), nn.GELU(), nn.Dropout(dropout), nn.Linear(d_ff, d_model))
        self.drop = nn.Dropout(dropout)

    def forward(self, x, ex):                       # x: (B, P+1, D) token toan cuc o cuoi; ex: (B, n_ex, D)
        x = self.n1(x + self.drop(self.sa(x, x, x, need_weights=False)[0]))
        if self.ca is not None:
            g = x[:, -1:, :]
            g = g + self.drop(self.ca(g, ex, ex, need_weights=False)[0])
            x = torch.cat([x[:, :-1, :], g], dim=1)
        x = self.n2(x)
        return self.n3(x + self.drop(self.ff(x)))


class TimeXer(nn.Module):
    """TimeXer (Wang et al., NeurIPS 2024, arXiv 2402.19072), ban tu cai dat gon.
    Bien noi sinh (kenh 0): cat thanh cac patch khong chong lan + 1 token toan cuc hoc duoc.
    Bien ngoai sinh (cac kenh con lai): moi bien thanh 1 token bang embedding tuyen tinh toan chuoi.
    Dau ra: flatten cac token noi sinh; co the noi them dac trung biet truoc (lich, ngay le) cua H ngay can du bao."""

    def __init__(self, n_feat, lookback, horizon, d_model=64, n_heads=4, n_layers=2, dropout=0.1,
                 patch_len=7, n_future=0, anchor="none"):
        super().__init__()
        self.n_future, self.patch_len, self.anchor, self.horizon = n_future, patch_len, anchor, horizon
        self.n_patches = lookback // patch_len
        self.used = self.n_patches * patch_len
        self.n_ex = n_feat - 1
        self.patch_embed = nn.Linear(patch_len, d_model)
        self.pos = nn.Parameter(torch.zeros(1, self.n_patches, d_model))
        nn.init.trunc_normal_(self.pos, std=0.02)
        self.glb = nn.Parameter(torch.randn(1, 1, d_model) * 0.02)
        self.ex_embed = nn.Linear(lookback, d_model) if self.n_ex > 0 else None
        self.layers = nn.ModuleList([_XLayer(d_model, n_heads, 2 * d_model, dropout, self.n_ex > 0) for _ in range(n_layers)])
        flat = (self.n_patches + 1) * d_model
        if n_future:
            self.head = nn.Sequential(nn.Flatten(), nn.Dropout(dropout), nn.Linear(flat + horizon * n_future, d_model),
                                      nn.GELU(), nn.Dropout(dropout), nn.Linear(d_model, horizon))
        else:
            self.head = nn.Sequential(nn.Flatten(), nn.Dropout(dropout), nn.Linear(flat, horizon))

    def forward(self, x, fut=None):
        t, m, s = _norm0(x)                                          # (B, L)
        p = t[:, -self.used:].unfold(1, self.patch_len, self.patch_len)   # (B, P, patch_len)
        h = self.patch_embed(p) + self.pos
        h = torch.cat([h, self.glb.expand(len(h), -1, -1)], dim=1)   # token toan cuc o cuoi
        ex = self.ex_embed(x[..., 1:].transpose(1, 2)) if self.n_ex > 0 else None
        for layer in self.layers:
            h = layer(h, ex)
        z = h.flatten(1)
        if self.n_future:
            z = torch.cat([z, fut.flatten(1)], dim=1)
        out = self.head(z) + _anchor(t, self.horizon, self.anchor)
        return out * s + m


class _Mamba(nn.Module):
    """Khoi Mamba (selective SSM, Gu & Dao 2023) viet thuan PyTorch, quet tuan tu (chay duoc tren CPU, khong can mamba-ssm/CUDA).
    Do dai chuoi o day la SO BIEN (token bien), nen vong lap ngan."""

    def __init__(self, d_model, d_state=16, expand=2, d_conv=4):
        super().__init__()
        d_inner = expand * d_model
        self.d_inner, self.d_state = d_inner, d_state
        self.dt_rank = max(1, math.ceil(d_model / 16))
        self.in_proj = nn.Linear(d_model, 2 * d_inner)
        self.conv = nn.Conv1d(d_inner, d_inner, d_conv, groups=d_inner, padding=d_conv - 1)
        self.x_proj = nn.Linear(d_inner, self.dt_rank + 2 * d_state, bias=False)
        self.dt_proj = nn.Linear(self.dt_rank, d_inner)
        self.A_log = nn.Parameter(torch.log(torch.arange(1, d_state + 1).float().repeat(d_inner, 1)))
        self.D = nn.Parameter(torch.ones(d_inner))
        self.out_proj = nn.Linear(d_inner, d_model)
        dt = torch.exp(torch.rand(d_inner) * (math.log(0.1) - math.log(1e-3)) + math.log(1e-3)).clamp(min=1e-4)
        with torch.no_grad():
            self.dt_proj.bias.copy_(dt + torch.log(-torch.expm1(-dt)))

    def forward(self, u):                                    # u: (B, T, D)
        Bsz, T, _ = u.shape
        x, z = self.in_proj(u).chunk(2, dim=-1)
        x = F.silu(self.conv(x.transpose(1, 2))[..., :T].transpose(1, 2))
        dt, Bm, Cm = torch.split(self.x_proj(x), [self.dt_rank, self.d_state, self.d_state], dim=-1)
        dt = F.softplus(self.dt_proj(dt))                    # (B, T, d_inner)
        A = -torch.exp(self.A_log)                           # (d_inner, d_state)
        h = x.new_zeros(Bsz, self.d_inner, self.d_state)
        ys = []
        for t in range(T):
            dA = torch.exp(dt[:, t, :, None] * A)
            h = dA * h + dt[:, t, :, None] * Bm[:, t, None, :] * x[:, t, :, None]
            ys.append((h * Cm[:, t, None, :]).sum(-1))
        y = torch.stack(ys, dim=1) + x * self.D
        return self.out_proj(y * F.silu(z))


class _SMLayer(nn.Module):
    def __init__(self, d_model, d_state, dropout):
        super().__init__()
        self.fwd, self.bwd = _Mamba(d_model, d_state), _Mamba(d_model, d_state)
        self.n1, self.n2 = nn.LayerNorm(d_model), nn.LayerNorm(d_model)
        self.ff = nn.Sequential(nn.Linear(d_model, 2 * d_model), nn.GELU(), nn.Dropout(dropout), nn.Linear(2 * d_model, d_model))
        self.drop = nn.Dropout(dropout)

    def forward(self, tok):                                  # tok: (B, n_bien, D)
        both = self.fwd(tok) + self.bwd(tok.flip(1)).flip(1)  # quet hai chieu qua cac bien
        tok = self.n1(tok + self.drop(both))
        return self.n2(tok + self.drop(self.ff(tok)))


class SMamba(nn.Module):
    """S-Mamba (Wang et al. 2024, arXiv 2403.11144), ban tu cai dat gon: moi bien -> 1 token (embedding tuyen tinh toan chuoi),
    cac lop Mamba hai chieu quet QUA CAC BIEN de hoc tuong quan giua cac bien, lay token cua bien target lam dau ra.
    Co the noi them dac trung biet truoc (lich, ngay le) cua H ngay can du bao."""

    def __init__(self, n_feat, lookback, horizon, d_model=64, n_layers=2, d_state=16, dropout=0.1, n_future=0, anchor="none"):
        super().__init__()
        self.n_future, self.anchor, self.horizon = n_future, anchor, horizon
        self.embed = nn.Linear(lookback, d_model)
        self.layers = nn.ModuleList([_SMLayer(d_model, d_state, dropout) for _ in range(n_layers)])
        self.norm = nn.LayerNorm(d_model)
        if n_future:
            self.head = nn.Sequential(nn.Linear(d_model + horizon * n_future, d_model), nn.GELU(), nn.Dropout(dropout),
                                      nn.Linear(d_model, horizon))
        else:
            self.head = nn.Linear(d_model, horizon)

    def forward(self, x, fut=None):
        t, m, s = _norm0(x)
        xs = torch.cat([t.unsqueeze(-1), x[..., 1:]], dim=-1)          # (B, L, F)
        tok = self.embed(xs.transpose(1, 2))                           # (B, F, D)
        for layer in self.layers:
            tok = layer(tok)
        z = self.norm(tok)[:, 0]                                       # token cua bien target
        if self.n_future:
            z = torch.cat([z, fut.flatten(1)], dim=1)
        return (self.head(z) + _anchor(t, self.horizon, self.anchor)) * s + m


def build_model(name, n_feat, horizon, lookback, a):
    """a: namespace tham so. Tham so tuy chon (co mac dinh): kernel, patch_len, stride, ptst_layers."""
    if name == "s4d":
        return S4Forecaster(n_feat, horizon, a.d_model, a.n_layers, a.d_state, a.dropout, a.dt_min, a.dt_max,
                            n_future=getattr(a, "n_future", 0), anchor=getattr(a, "anchor", "none"))
    if name == "dlinear":
        return DLinear(lookback, horizon, kernel=getattr(a, "kernel", 25))
    if name == "patchtst":
        return PatchTST(lookback, horizon, patch_len=getattr(a, "patch_len", 8), stride=getattr(a, "stride", 4),
                        d_model=a.d_model, n_layers=getattr(a, "ptst_layers", 2), dropout=a.dropout)
    if name == "timexer":
        return TimeXer(n_feat, lookback, horizon, d_model=a.d_model, n_layers=getattr(a, "ptst_layers", getattr(a, "n_layers", 2)),
                       dropout=a.dropout, patch_len=getattr(a, "patch_len", 7), n_future=getattr(a, "n_future", 0),
                       anchor=getattr(a, "anchor", "none"))
    if name == "smamba":
        return SMamba(n_feat, lookback, horizon, d_model=a.d_model, n_layers=a.n_layers, d_state=a.d_state,
                      dropout=a.dropout, n_future=getattr(a, "n_future", 0), anchor=getattr(a, "anchor", "none"))
    raise ValueError(f"mo hinh khong biet: {name}")

@torch.no_grad()
def check_recurrence(d_model=3, d_state=8, L=24, seed=0):
    """Kiem tra: tich chap (train) va de quy (suy luan) cho cung ket qua."""
    torch.manual_seed(seed)
    ker = S4DKernel(d_model, d_state)
    K = ker(L)                                           # (H, L)
    _, A_bar, C = ker.discretize()                       # C da gom B_bar
    u = torch.randn(d_model, L)
    # dang tich chap: y[t] = sum_j K[j] * u[t-j]
    y_conv = torch.stack([sum(K[:, j] * u[:, t - j] for j in range(t + 1)) for t in range(L)], dim=1)
    # dang de quy: x_t = A_bar * x_{t-1} + u_t ;  y_t = 2 Re(C . x_t)
    x = torch.zeros(d_model, d_state // 2, dtype=torch.cfloat)
    ys = []
    for t in range(L):
        x = A_bar * x + u[:, t:t + 1]
        ys.append(2 * (C * x).sum(-1).real)
    y_rec = torch.stack(ys, dim=1)
    err = (y_conv - y_rec).abs().max().item()
    print(f"max |conv - recurrence| = {err:.2e}  ->", "OK" if err < 1e-4 else "SAI")
    return err


if __name__ == "__main__":
    check_recurrence()
    m = S4Forecaster(n_feat=10, horizon=1)
    print("so tham so:", sum(p.numel() for p in m.parameters()))
    print("dau ra:", m(torch.randn(4, 56, 10)).shape)
