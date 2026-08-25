# boundedrun — specifikacija

> **Za sesiju koja počinje bez konteksta:** ovaj dokument je potpun. Pročitaj §0 pa §20.

---

## 0. Kontekst — zašto ovaj repozitorij postoji

**Autor:** Tihomir Tomašević, softverski arhitekt, 17 godina iskustva u
enterprise sustavima (Ericsson, IBM), danas vodi razvoj agentic LLM platforme u
američkoj SaaS tvrtki. Gradio je i vlastitu platformu za validaciju dokumenata
po točno ovom principu — LLM zatvoren u korak prosudbe, sve oko njega
konvencionalno i testabilno.

**Svrha:** javni portfeljni projekt koji dokazuje tezu autorovog članka. Cilj
nije proizvod ni korisnici, nego da tehnički recenzent u dvije minute vidi da
autor zna kada agent **ne** treba. Publika: CTO-i i staff inženjeri.

**Posljedice za razvoj:**

- **Manje je bolje.** Ovo mora biti manje od bilo kojeg agent frameworka —
  to je i argument. Ako pređe ~900 redaka jezgre, nešto je krivo.
- **Ne gradi workflow engine.** Vidi §3.
- **Kod čitljiv na prvi pogled.** Recenzent skenira, ne proučava.
- **Nema neprovjerljivih tvrdnji.** Autorovo pravilo s vlastitog sajta.
- **Glas je prvo lice jednine.** „I", nikad „we".

**Srodni projekti:** `fivegates`, `driftline`, `recallgap`. Vidi §14 — veza s
`fivegates`om je konceptualno najvažnija u cijelom portfelju.

## 0.1 Sažetak članka na kojem projekt počiva

Prateći tekst: *Before You Build an Agent, Try a Cron Job*
(https://t2software.ai/writing/before-you-build-an-agent.html).

1. **Jedno pitanje odlučuje sve:** *„does the sequence of steps need to vary per
   input?"* Ako ne, agent nije potreban.
2. **Ljestve od četiri prečke**, po rastućoj složenosti:
   1. obični kod — tok kontroliraš pri pisanju
   2. **jedan poziv modela u fiksnom pipelineu** — model puni vrijednosti
   3. **fiksni workflow s više koraka modela** — više točaka prosudbe
   4. agent — model kontrolira tok u vrijeme izvođenja, neomeđen najgori slučaj
3. **Granica je: *„who decides what happens next"*.** Prečke 1–3 = ti, u
   čitljivom kodu. Prečka 4 = vjerojatnosna odluka u runtimeu.
4. **Što se dobiva ostankom na nižim prečkama:** *„a bounded worst case"*,
   kvarovi slijedivi do retka koda, jedna evaluacijska površina po pozivu
   modela, *„latency you can quote"*, i kod koji svaki inženjer održava.
5. **Agent zaslužuje složenost** uz dva ili više ovih signala: nepoznat broj
   koraka do runtimea, nenabrojive kombinacije alata, semantički uvjet
   zaustavljanja, i najjači — *„a human is in the loop, steering"*.
6. **Ne opravdavaju agenta:** težina zadatka, nestrukturiran tekst, prirodan
   izlaz, složenost domene. To traži jezični model, ne agentski tok kontrole.
7. **Obrazac kvara:** ambiciozne arhitekture koje sjajno demonstriraju pa ih
   *„get quietly switched off eight months later"*.
8. **Preporučeni put:** izgradi determinističku verziju, instrumentiraj je,
   pusti na pravi promet, analiziraj **koje je konkretne odluke kruti tok
   promašio**, i tek onda se penji — naoružan evaluacijskim skupom iz stvarnih
   kvarova, mjerljivim baselineom i fallback putevima.
9. ***„Agentic is a control-flow decision, not a product decision."***

Točke 2, 4 i 8 su opseg ovog repozitorija.

---

## 1. Naziv

`boundedrun` — *bounded worst case* je konkretna korist ostanka na prečkama 2–3
i glavna značajka alata: najgori slučaj se zna **prije** pokretanja (§5).

**Ime provjereno 25.08.2026:** slobodno na PyPI-ju, na GitHubu dva repozitorija
tog imena, oba s nula zvjezdica. Ne preispituj bez razloga.

> Slobodne alternative: `stepladder` (ali postoji GitHub repo sa 155★), `rung`,
> `fixedflow`, `cronfirst`, `secondrung`, `judgestep`. **Zauzeto:** `rungs`,
> `plainflow`.

## 2. Teza

Većina timova skoči na prečku 4 jer prečke 2 i 3 nemaju alat. Postoji desetak
agent frameworka i gotovo ništa za *„pipeline with a language model in the
middle"* — pa je agent put manjeg otpora, iako je gotovo uvijek pogrešan.

`boundedrun` daje toj razini alat, i uz njega dvije stvari koje agent po
definiciji ne može imati:

1. **Statički omeđen najgori slučaj** — broj poziva modela, tokeni, trošak i
   latencija poznati prije pokretanja, jer je niz koraka fiksan.
2. **Izvještaj o dozrijevanju** (§9) — mjeri koliko je puta kruti tok bio
   pogrešan, i time daje **dokaz** za penjanje na prečku 4, umjesto dojma.

## 3. Što ovo NIJE

- **Nije workflow engine.** Airflow, Prefect, Temporal, Dagster orkestriraju
  proizvoljne DAG-ove u distribuiranom okruženju. `boundedrun` je mala
  in-process biblioteka za jedan linearni pipeline s koracima prosudbe. Ako
  trebaš skaliranje, raspored po satu i backfill — uzmi Temporal.
- **Nije agent framework.** Namjerno. Za prečku 4 postoji `fivegates` (§14).
- **Nije LLM klijent.** Poziv modela je funkcija koju predaš.
- **Nije eval framework.** Replay (§8) služi usporedbi verzija prompta, ne
  ocjenjivanju kvalitete.
- **Nema grananja ovisnog o modelu.** To je prečka 4 i **eksplicitno je izvan
  dosega** — čim model odlučuje što je sljedeće, više nisi ovdje.

## 4. Načela

**Niz koraka je fiksan i deklariran.** Grananje smije ovisiti o
determinističkim uvjetima, nikad o slobodnoj odluci modela. Ovo je granica koja
definira projekt.

**Nedeterminizam u najmanjoj kutiji.** Korak prosudbe prima tipiziran ulaz i
vraća izlaz po shemi. Sve oko njega — red čekanja, retry, perzistencija,
idempotentnost, revizijski trag — konvencionalno je i testabilno.

**Async-first.** Koraci su I/O-bound; API je async od prvog retka.

**Svaki run je reproducibilan.** Zapisani ulazi i verzije prompta dovoljni su
da se run ponovi bajt po bajt tamo gdje je deterministički.

## 5. Omeđen najgori slučaj — glavna značajka

Jer je niz koraka poznat pri deklaraciji, granice se računaju statički:

```python
bounds = pipeline.bounds()
print(bounds)
```

```
boundedrun: 6 koraka (4 deterministička, 2 prosudbe)
  najgori slučaj, s retryjima (max 3 po koraku):
    poziva modela        6
    ulazni tokeni    18,000
    izlazni tokeni    3,000
    trošak            $0.14
    latencija          ~5.2 s
  bez retryja:  2 poziva, $0.05, ~1.7 s
```

Ovo je *„latency you can quote"* pretvoreno u naredbu. Agent to ne može dati
jer mu je broj koraka nepoznat do runtimea — i upravo tu razliku README mora
istaknuti.

Granice se mogu i **provoditi**: `Pipeline(enforce_bounds=True)` diže iznimku
ako run premaši deklarirani strop, umjesto da tiho poskupi.

CLI ih ispisuje bez pokretanja pipelinea, pa mogu u CI kao zaštita od
neprimijećenog rasta:

```
boundedrun bounds mypkg.flows:classify --max-cost 0.20
```

## 6. Deklaracija pipelinea

```python
from boundedrun import Pipeline, step, judgment


@step
async def extract(ctx, pdf: bytes) -> str:
    return await pdf_to_text(pdf)  # deterministički


@judgment(
    prompt_version="classify@v7",
    output_schema={"type": "object", "required": ["category", "confidence"]},
    max_tokens=1_200,
    retries=2,
)
async def classify(ctx, text: str) -> dict:
    return await ctx.model(prompt=CLASSIFY.format(text=text))


@step
async def validate(ctx, result: dict) -> dict:
    if result["confidence"] < 0.7:
        raise ctx.NeedsReview("niska pouzdanost")  # deterministički izlaz
    return result


pipeline = Pipeline(
    name="doc-classify",
    steps=[extract, classify, validate, persist],
    store="./runs.db",
    model=my_async_model,
)

result = await pipeline.run(pdf_bytes, idempotency_key="doc-4471")
```

Determinističko grananje je dopušteno i deklarirano unaprijed:

```python
steps=[extract, classify, branch(on="category", {
    "invoice":  [extract_totals, validate_totals],
    "contract": [extract_parties],
})]
```

**Grananje po slobodnoj odluci modela nije podržano i neće biti.** Ako ti treba,
popeo si se na prečku 4 — vidi §14.

## 7. Trajnost i revizijski trag

Svaki korak zapisuje ulaz, izlaz, trajanje i ishod. Pipeline se može nastaviti
iz zadnjeg uspješnog koraka nakon pada procesa.

- **Idempotentnost**: `idempotency_key` po runu; ponovni poziv s istim ključem
  vraća prvi rezultat umjesto da ponovi posao.
- **Retry po koraku**, s politikom deklariranom na koraku, ne globalno.
- **Revizijski zapis koraka prosudbe**: ulaz, verzija prompta, model, izlaz,
  broj tokena. To je razlika između sustava koji smije dirati pravi novac i onog
  koji ne smije.
- **`NeedsReview`** je prvorazredan ishod, ne iznimka koja se guta — run
  završava u stanju `needs_review` s očuvanim kontekstom.

## 8. Replay

Zapisani runovi se mogu ponoviti protiv druge verzije prompta ili modela, a
razlike ispisati:

```
boundedrun replay --since 30d --prompt classify@v8

142 runa ponovljeno
  ishod isti          131  (92%)
  ishod promijenjen    11   (8%)
      invoice -> receipt     7
      contract -> invoice    4
  trošak: $0.11 -> $0.09 po runu
```

Ovo je *„evaluation set from real failures"* iz članka, samo nastalo samo od
sebe — bez ručnog sastavljanja skupa. Determinističke korake replay preskače.

## 9. Izvještaj o dozrijevanju — diferencijator

Članak kaže: penji se na prečku 4 tek kad analiziraš *„the specific decisions
the rigid flow got wrong."* Nitko za to nema alat, pa se odluka donosi po
dojmu. Ovo ga daje.

Pipeline prikuplja **signale nesklada**, svaki deterministički zabilježen:

| signal | što znači |
|---|---|
| `needs_review` udio | kruti tok prečesto odustaje |
| ručna korekcija ishoda | čovjek je mijenjao rezultat nakon izvođenja |
| ponovljena putanja grananja | ista grana uvijek pogrešna za neku klasu ulaza |
| korak preskočen kao nepotreban | fiksni niz radi suvišan posao |
| **zatražen korak koji ne postoji** | korak prosudbe vraća „trebao bih X", a X nije u pipelineu |

```
boundedrun graduation --since 90d

2,431 run
  signali nesklada u 104 runa (4.3%)
     zatražen nepostojeći korak     61   uglavnom "dohvati prethodni ugovor"
     ručna korekcija                29
     needs_review                   14
  
  Preporuka: 4.3% je ispod praga. Ostani na prečki 3.
  Ako pređe 15%, imaš dokaz za penjanje — i evaluacijski skup od 104 runa
  s kojim počinješ.
```

Prag je konfigurabilan i **namjerno visok**. Alat je pristran prema
jednostavnosti; to je poanta članka.

## 10. Struktura repozitorija

```
boundedrun/
  README.md            teza, ljestve, izlaz `bounds` iz §5, veza na članak
  pyproject.toml
  src/boundedrun/
    pipeline.py        deklaracija, izvođenje, determinističko grananje
    steps.py           @step i @judgment, sheme, retry politike
    bounds.py          statički izračun najgoreg slučaja
    store.py           SQLite: runovi, koraci, revizijski trag
    replay.py          §8
    graduation.py      §9
    cli.py             boundedrun bounds / runs / show / replay / graduation
  examples/
    01_classify.py     pipeline s lažnim modelom, offline
    02_bounds.py       dodavanje koraka mijenja objavljeni najgori slučaj
    03_graduation.py   pipeline koji prerasta sebe — signali rastu preko praga
  tests/
  docs/DESIGN.md       ljestve, s citatima iz članka
```

## 11. Milestoneovi

| | opseg | rezultat | procjena |
|---|---|---|---|
| **M0** | deklaracija, izvođenje, store, retry, idempotentnost | pipeline radi i preživi pad | 1 vikend |
| **M1** | `bounds()` i CLI, `NeedsReview`, primjeri 01+02, README | **repozitorij je pokaziv** | +1 vikend |
| **M2** | revizijski trag, replay (§8) | usporedba verzija prompta radi | +1 vikend |
| **M3** | izvještaj o dozrijevanju (§9), determinističko grananje, primjer 03 | diferencijator zaokružen | +1–2 vikenda |

Iznimka od uobičajenog redoslijeda: **`bounds()` ide već u M1** jer je to
najuvjerljiviji dio i nosi README.

## 12. README javnog repozitorija — nacrt

1. Jedna rečenica: *Most systems that call themselves agents are pipelines with a model in the middle. This is the tooling for that.*
2. **Ljestve od četiri prečke** kao tablica — odmah, prije teksta
3. Izlaz `bounds()` iz §5 — *latency you can quote*
4. 60-sekundni primjer (kod iz §6)
5. Izvještaj o dozrijevanju iz §9 — kada prijeći na agenta i zašto ovaj alat to mjeri
6. Što ovo nije (§3) — posebno razgraničenje od Temporala i agent frameworka
7. Instalacija, status, licenca (MIT)

## 13. Otvorena pitanja

- **Procjena latencije u `bounds()`**: statična po modelu ili izmjerena iz
  povijesnih runova? Statična je odmah dostupna ali netočna; izmjerena je
  vjerodostojna ali traži prethodne runove. Prijedlog: statična kao default,
  automatski zamijenjena izmjerenom kad postoji ≥30 runova, i **jasno označeno
  koja se koristi**.
- **Determinističko grananje i `bounds()`**: najgori slučaj po granama može se
  jako razlikovati. Prijedlog: prijavi najgoru granu, uz raspon.
- **Paralelni koraci**: smiju li se nezavisni koraci vrtjeti usporedno? Da, ali
  tek u M3, i samo uz eksplicitnu oznaku — inače latencija prestaje biti
  predvidljiva na način koji se može citirati.

## 14. Odnos prema `fivegates` — najvažnija veza u portfelju

`boundedrun` je **prečke 2–3**. `fivegates` je **prečka 4**. Zajedno su ljestve
iz članka, i to je namjerno.

Izvještaj o dozrijevanju (§9) je most: kad signali nesklada prijeđu prag,
`boundedrun` ti kaže da si prerastao fiksni tok — a `fivegates` je mjesto na
koje se penješ, s evaluacijskim skupom koji si usput skupio.

README oba projekta trebaju upućivati jedan na drugi, s jednom rečenicom o tome
koju prečku pokrivaju. Za recenzenta je to najjači signal u cijelom portfelju:
**autor ne prodaje agente, nego zna kada ih ne treba** — i izgradio je alat za
obje strane te odluke.

`driftline` promatra oboje u produkciji; `recallgap` dijagnosticira dohvat prije
nego išta od toga ode u rad.

## 15. Provjera pozicioniranja

A.Team izrijekom odbija *„developers focused primarily on simple AI wrappers or
templated implementations."* Ovaj projekt je najjači protuprimjer u portfelju:
argument mu je **protiv** složenosti koja se trenutno najbolje prodaje. Alat
koji odgovara vlasnika od agenta pokazuje prosudbu koju nijedan demo ne može.

## 16. Tehničke odluke (ne preispituj bez razloga)

| odluka | izbor | zašto |
|---|---|---|
| jezik | Python 3.11+ | autorov stack; `TaskGroup` za M3 |
| model izvođenja | **async-first**, `asyncio` | koraci su I/O-bound |
| pohrana | **SQLite, stdlib `sqlite3`**, WAL, kroz `asyncio.to_thread` | jedna datoteka, preživi pad |
| validacija sheme | `jsonschema` | isto kao `fivegates`, dosljedno u portfelju |
| CLI | `typer` | |
| testovi | `pytest` + `anyio` | |
| licenca | MIT | |
| formatiranje | `ruff` | |

Osnovne ovisnosti: `typer`, `jsonschema`. **Bez `pydantic`**, bez orkestracijskih
biblioteka, bez ijednog agent frameworka — to bi proturječilo tezi.

## 17. Shema pohrane

```sql
CREATE TABLE runs (
  run_id          TEXT PRIMARY KEY,
  pipeline        TEXT NOT NULL,
  idempotency_key TEXT,
  status          TEXT NOT NULL,   -- running|done|failed|needs_review|bounds_exceeded
  started_at      TEXT NOT NULL,
  ended_at        TEXT,
  cost_usd        REAL DEFAULT 0,
  tokens_in       INTEGER DEFAULT 0,
  tokens_out      INTEGER DEFAULT 0,
  input_hash      TEXT
);
CREATE UNIQUE INDEX ix_runs_idem ON runs(pipeline, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE run_steps (
  step_run_id  TEXT PRIMARY KEY,
  run_id       TEXT NOT NULL REFERENCES runs(run_id),
  seq          INTEGER NOT NULL,
  step_name    TEXT NOT NULL,
  kind         TEXT NOT NULL,      -- deterministic | judgment
  attempt      INTEGER NOT NULL DEFAULT 1,
  status       TEXT NOT NULL,      -- ok | retried | failed | skipped
  input_json   TEXT,
  output_json  TEXT,
  prompt_version TEXT,             -- samo judgment
  model        TEXT,
  tokens_in    INTEGER,
  tokens_out   INTEGER,
  latency_ms   INTEGER,
  error        TEXT,
  started_at   TEXT NOT NULL,
  ended_at     TEXT
);

CREATE TABLE misfit_signals (            -- §9
  signal_id  TEXT PRIMARY KEY,
  run_id     TEXT NOT NULL REFERENCES runs(run_id),
  kind       TEXT NOT NULL,        -- missing_step | manual_correction |
                                   -- needs_review | branch_wrong | step_skipped
  detail     TEXT,
  recorded_at TEXT NOT NULL
);

CREATE INDEX ix_steps_run   ON run_steps(run_id, seq);
CREATE INDEX ix_misfit_kind ON misfit_signals(kind, recorded_at);
```

## 18. Kriteriji dovršenosti

**M0**
- [ ] pipeline izvodi korake redom, s lažnim modelom, bez API ključa
- [ ] `kill -9` nasred runa → nastavak iz zadnjeg uspješnog koraka
- [ ] isti `idempotency_key` vraća prvi rezultat, ne ponavlja posao
- [ ] retry politika po koraku, ne globalna
- [ ] `@judgment` izlaz koji ne odgovara shemi → retry, pa `failed`

**M1**
- [ ] `pipeline.bounds()` daje izlaz iz §5, izračunat **bez pokretanja**
- [ ] `enforce_bounds=True` diže iznimku pri prekoračenju, run ide u `bounds_exceeded`
- [ ] `NeedsReview` završava run u `needs_review` s očuvanim kontekstom
- [ ] `examples/02_bounds.py` pokazuje da dodan korak mijenja objavljeni broj
- [ ] README po nacrtu iz §12

**M2**
- [ ] revizijski zapis svakog `judgment` koraka potpun (ulaz, verzija, model, izlaz, tokeni)
- [ ] `boundedrun replay` daje izlaz iz §8 i **ne poziva model za determinističke korake**
- [ ] replay ne mijenja izvorne runove

**M3**
- [ ] svih pet signala nesklada se bilježi
- [ ] `boundedrun graduation` daje izlaz iz §9 s preporukom
- [ ] determinističko grananje radi, `bounds()` prijavljuje najgoru granu
- [ ] `examples/03_graduation.py` prelazi prag i preporuka se mijenja

## 19. Testna strategija

- **Bez pravih API poziva.** Lažni async model s namještenim izlazima i
  brojevima tokena. Cijeli paket radi offline.
- **`bounds()` se testira protiv stvarnog izvođenja**: pokreni pipeline u
  najgorem scenariju (svi retryji iscrpljeni) i provjeri da stvarni trošak i
  broj poziva **ne premašuju** objavljenu granicu. Ovo je najvažniji test — ako
  granica nije istinita, cijela teza pada.
- **Pad procesa se testira stvarno**, `subprocess` koji se ubije.
- **Idempotentnost pod istovremenošću**: dva paralelna runa s istim ključem →
  jedan izvršen, drugi dobiva isti rezultat.
- Ciljaj ~35 testova.

## 20. Prvi korak u novoj sesiji

1. `git init`, `pyproject.toml`, `ruff`, `pytest`, MIT
2. `store.py` — shema iz §17, WAL
3. `steps.py` — `@step` i `@judgment` dekoratori, metapodaci koje `bounds()` treba
4. `bounds.py` — statički izračun; **napiši rano**, jer diktira koje metapodatke
   koraci moraju nositi
5. `pipeline.py` — izvođenje, retry, idempotentnost
6. test iz §19 koji uspoređuje granicu sa stvarnim najgorim izvođenjem

Ne piši replay, grananje ni izvještaj o dozrijevanju dok M0 ne prolazi.
