# 📋 Rapport des Transformations — Pipeline ETL Mexora

> Ce document recense **toutes les règles de transformation** appliquées  
> lors de l'exécution du pipeline ETL Mexora.  
> Pour chaque règle : la justification métier, le code Python appliqué,  
> et le nombre de lignes affectées (issu des logs d'exécution).

---

## Résumé global

| Source | Lignes brutes | Lignes après nettoyage | Lignes supprimées |
|--------|:-------------:|:----------------------:|:-----------------:|
| `commandes_mexora.csv` | 51 500 | 48 785 | 2 715 |
| `clients_mexora.csv` | 5 188 | 5 000 | 188 |
| `produits_mexora.json` | 50 | 50 | 0 |
| `regions_maroc.csv` | 30 | 30 | 0 (référentiel propre) |

---

## 1. Nettoyage des Commandes (`clean_commandes.py`)

---

### Règle C-01 — Suppression des doublons sur `id_commande`

**Règle métier**  
Une commande est identifiée de façon unique par son `id_commande`.  
Les doublons résultent d'erreurs de saisie ou de ré-insertions accidentelles  
dans le système transactionnel source. On conserve la première occurrence.

**Code appliqué**
```python
df = df.drop_duplicates(subset=["id_commande"], keep="first")
```

**Lignes affectées : 1 500 lignes supprimées**

---

### Règle C-02 — Normalisation des formats de dates

**Règle métier**  
Le système source utilise plusieurs formats de date selon les modules  
(web, mobile, import manuel). Tous doivent être convertis en `datetime`  
standard pour permettre la jointure avec `dim_temps`.

**Formats détectés**

| Format source | Exemple | Fréquence |
|---------------|---------|-----------|
| `YYYY-MM-DD` | `2024-11-15` | ~50% |
| `DD/MM/YYYY` | `15/11/2024` | ~20% |
| `Mon DD YYYY` | `Nov 15 2024` | ~10% |
| `DD-MM-YYYY` | `15-11-2024` | ~10% |
| `YYYY/MM/DD` | `2024/11/15` | ~10% |

**Code appliqué**
```python
def _parse_date(val):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%b %d %Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return pd.to_datetime(val, format=fmt)
        except ValueError:
            continue
    return pd.NaT

df["date_commande"] = df["date_commande"].apply(_parse_date)
df = df.dropna(subset=["date_commande"])
```

**Lignes affectées : 0 date invalide** (tous les formats ont été reconnus)

---

### Règle C-03 — Harmonisation de `ville_livraison`

**Règle métier**  
La ville de livraison est saisie manuellement par plusieurs opérateurs,  
générant des incohérences orthographiques. Le référentiel `regions_maroc.csv`  
est utilisé comme table de correspondance officielle.

**Incohérences détectées**

| Valeur brute | Valeur standard |
|---|---|
| `Tanger`, `TNG`, `TANGER`, `Tnja`, `Tnger` | `Tanger` |
| `Fes`, `FES`, `Fez`, `fes` | `Fès` |
| `Casa`, `CASABLANCA`, `casablanca` | `Casablanca` |
| `Kenitra`, `KENITRA` | `Kénitra` |
| `Sale`, `SALE` | `Salé` |
| `Meknes`, `MEKNES` | `Meknès` |

**Code appliqué**
```python
mapping_villes = {}
for _, row in df_regions.iterrows():
    standard = row["nom_ville_standard"]
    mapping_villes[standard.lower()] = standard
    mapping_villes[row["code_ville"].lower()] = standard

df["ville_livraison"] = df["ville_livraison"].apply(
    lambda v: mapping_villes.get(v.strip().lower(), v.strip().title())
)
```

**Lignes affectées : 48 785 lignes harmonisées**

---

### Règle C-04 — Remplacement des `id_livreur` manquants

**Règle métier**  
7% des commandes n'ont pas de livreur assigné (commandes en préparation  
ou données manquantes à la migration). On remplace par `"INCONNU"`  
pour conserver ces commandes dans l'analyse sans perte de données.

**Code appliqué**
```python
df["id_livreur"] = df["id_livreur"].replace("", "INCONNU").fillna("INCONNU")
```

**Lignes affectées : 3 499 valeurs remplacées par "INCONNU"**

---

### Règle C-05 — Suppression des quantités négatives ou nulles

**Règle métier**  
Une quantité négative ou nulle n'a pas de sens commercial.  
Ces lignes sont des erreurs de saisie et doivent être écartées  
avant tout calcul de chiffre d'affaires.

**Code appliqué**
```python
df["quantite"] = pd.to_numeric(df["quantite"], errors="coerce")
df = df[df["quantite"] > 0]
```

**Lignes affectées : 717 lignes supprimées**

---

### Règle C-06 — Suppression des commandes avec `prix_unitaire = 0`

**Règle métier**  
Un prix à zéro indique une commande test ou une erreur de saisie.  
Ces lignes fausseraient le calcul du chiffre d'affaires et du panier moyen.

**Code appliqué**
```python
df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"], errors="coerce")
df = df[df["prix_unitaire"] > 0]
```

**Lignes affectées : 498 lignes supprimées**

---

### Règle C-07 — Normalisation des statuts non-standards

**Règle métier**  
Le système source accepte des valeurs libres pour le statut.  
Un mapping vers un référentiel contrôlé est nécessaire pour  
les filtres du dashboard.

**Mapping appliqué**

| Valeur source | Valeur normalisée |
|---|---|
| `OK`, `DONE`, `LIVREE`, `validated`, `oui`, `1` | `livree` |
| `KO` | `annulee` |
| `en_cours`, `annulee`, `retournee`, `expedie`, `en_preparation` | inchangé |

**Code appliqué**
```python
STATUT_MAP = {
    "ok": "livree", "done": "livree", "livree": "livree",
    "ko": "annulee", "oui": "livree", "1": "livree", "validated": "livree",
    "en_cours": "en_cours", "annulee": "annulee",
    "retournee": "retournee", "expedie": "expedie",
    "en_preparation": "en_preparation",
}
df["statut"] = df["statut"].str.strip().str.lower().map(STATUT_MAP).fillna("inconnu")
```

**Lignes affectées : 0 statut non mappé** (tous couverts par le mapping)

---

### Règle C-08 — Calcul du montant ligne

**Règle métier**  
Le montant par ligne de commande est la mesure principale  
de la table de faits. Il est calculé directement dans la transformation.

**Code appliqué**
```python
df["montant_ligne"] = (df["quantite"] * df["prix_unitaire"]).round(2)
```

**Lignes affectées : 48 785 lignes enrichies**

---

### Règle C-09 — Flag `a_retour`

**Règle métier**  
Indicateur binaire pour calculer rapidement le taux de retour  
par région, catégorie ou période, sans jointure supplémentaire.

**Code appliqué**
```python
df["a_retour"] = (df["statut"] == "retournee").astype(int)
```

**Lignes affectées : 48 785 lignes enrichies**

---

## 2. Nettoyage des Clients (`clean_clients.py`)

---

### Règle CL-01 — Déduplication par email

**Règle métier**  
Un même client peut apparaître avec deux `id_client` différents  
suite à une erreur de migration (doublon système).  
L'email est l'identifiant métier unique. On garde l'inscription la plus récente.

**Code appliqué**
```python
df["email_norm"] = df["email"].str.strip().str.lower()
df = df.sort_values("date_inscription", ascending=False)
df = df.drop_duplicates(subset=["email_norm"], keep="first")
```

**Lignes affectées : 188 doublons supprimés** (5 188 → 5 000)

---

### Règle CL-02 — Normalisation du sexe

**Règle métier**  
Le champ `sexe` est codé différemment selon la source d'import.  
On normalise vers un référentiel binaire `M / F / Inconnu`.

**Encodages détectés et mappés**

| Valeur source | Valeur normalisée |
|---|---|
| `H`, `m`, `M`, `Homme`, `male`, `1` | `M` |
| `F`, `f`, `Femme`, `female`, `0`, `2`, `fe` | `F` |
| Toute autre valeur | `Inconnu` |

**Code appliqué**
```python
SEXE_MAP = {
    "h":"M", "m":"M", "1":"M", "homme":"M", "male":"M",
    "f":"F", "0":"F", "femme":"F", "female":"F", "fe":"F", "2":"F",
}
df["sexe"] = df["sexe"].apply(
    lambda v: SEXE_MAP.get(str(v).strip().lower(), "Inconnu")
)
```

**Lignes affectées : 5 000 lignes normalisées**

---

### Règle CL-03 — Validation des dates de naissance

**Règle métier**  
Une date de naissance donnant un âge négatif (date dans le futur)  
ou supérieur à 120 ans est invalide. Ces valeurs sont mises à `NaT`  
pour éviter des biais dans les analyses démographiques.

**Code appliqué**
```python
df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")
today = pd.Timestamp.today()
masque_invalide = (
    df["date_naissance"].notna() &
    (
        ((today - df["date_naissance"]).dt.days / 365.25 < 0) |
        ((today - df["date_naissance"]).dt.days / 365.25 > 120)
    )
)
df.loc[masque_invalide, "date_naissance"] = pd.NaT
```

**Lignes affectées : 187 dates invalides mises à NaT**

---

### Règle CL-04 — Validation et flag des emails

**Règle métier**  
Les emails mal formatés (sans `@`, sans domaine) ne permettent  
pas d'envoyer des communications clients. On les détecte et on pose  
un flag `email_valide` sans supprimer la ligne.

**Code appliqué**
```python
import re
EMAIL_REGEX = re.compile(r'^[\w.+-]+@[\w-]+\.[a-z]{2,}$', re.IGNORECASE)
df["email_valide"] = df["email"].apply(
    lambda e: bool(EMAIL_REGEX.match(str(e).strip())) if e else False
)
```

**Lignes affectées : 399 emails invalides détectés et flaggés**

---

### Règle CL-05 — Harmonisation des villes clients

**Règle métier**  
Même logique que pour les commandes : harmonisation via  
le référentiel `regions_maroc.csv`.

**Code appliqué**
```python
df["ville"] = df["ville"].apply(
    lambda v: mapping_villes.get(v.strip().lower(), v.strip().title())
)
```

**Lignes affectées : 5 000 lignes harmonisées**

---

### Règle CL-06 — Enrichissement région administrative

**Règle métier**  
La région administrative est dérivée de la ville via jointure  
avec `regions_maroc.csv`. Permet les analyses par région (Casablanca-Settat,  
Tanger-Tétouan, etc.) sans dépendre du champ ville brut.

**Code appliqué**
```python
ref = df_regions[["nom_ville_standard", "region_admin", "zone_geo"]].copy()
ref.columns = ["ville", "region_admin", "zone_geo"]
df = df.merge(ref, on="ville", how="left")
df["region_admin"] = df["region_admin"].fillna("Autre Maroc")
```

**Lignes affectées : 5 000 lignes enrichies avec region_admin et zone_geo**

---

## 3. Nettoyage des Produits (`clean_produits.py`)

---

### Règle P-01 — Normalisation de la casse des catégories

**Règle métier**  
Le catalogue produits est alimenté par plusieurs équipes  
(acheteurs, équipe tech, import fournisseur), générant des incohérences  
de casse sur `categorie`, `marque`, etc. On normalise en title case.

**Incohérences détectées**

| Valeur source | Valeur normalisée |
|---|---|
| `electronique`, `ELECTRONIQUE` | `Electronique` |
| `alimentation`, `ALIMENTATION` | `Alimentation` |
| `electromenager`, `ELECTROMENAGER` | `Electromenager` |
| `mode`, `MODE` | `Mode` |
| `sport`, `SPORT` | `Sport` |

**Code appliqué**
```python
for col in ["categorie", "sous_categorie", "marque", "fournisseur", "origine_pays"]:
    df[col] = df[col].str.strip().str.title()
```

**Lignes affectées : 50 lignes normalisées**

---

### Règle P-02 — Gestion des produits inactifs (SCD)

**Règle métier**  
Des produits avec `actif=false` ont des commandes associées  
dans l'historique. Les supprimer briserait l'intégrité référentielle  
de la table de faits. On les **conserve** avec leur flag `actif=False`  
pour respecter la logique SCD (Slowly Changing Dimension).

**Code appliqué**
```python
df["actif"] = df["actif"].map({"True": True, "False": False})
# Pas de suppression — log uniquement
n_inactifs = (~df["actif"]).sum()
logger.info(f"Produits inactifs conservés pour SCD : {n_inactifs}")
```

**Lignes affectées : 6 produits inactifs conservés**

---

### Règle P-03 — Gestion des prix catalogue nuls

**Règle métier**  
Certains anciens produits ont un `prix_catalogue = null`  
(données manquantes à la migration). On remplace par `0.0` et on pose  
un flag `prix_manquant = 1` pour les identifier dans les rapports.

**Code appliqué**
```python
df["prix_catalogue"] = pd.to_numeric(df["prix_catalogue"], errors="coerce")
df["prix_manquant"]  = df["prix_catalogue"].isna().astype(int)
df["prix_catalogue"] = df["prix_catalogue"].fillna(0.0)
```

**Lignes affectées : 5 produits avec prix_manquant = 1**

---

### Règle P-04 — Calcul des tranches de prix

**Règle métier**  
Segmenter les produits par tranche de prix permet des analyses  
de performance par gamme (entrée de gamme vs premium).  
Tranches définies en MAD (Dirham marocain).

**Code appliqué**
```python
bins   = [0, 100, 500, 1000, 5000, float("inf")]
labels = ["< 100 MAD", "100-500 MAD", "500-1000 MAD", "1000-5000 MAD", "> 5000 MAD"]
df["tranche_prix"] = pd.cut(df["prix_catalogue"], bins=bins, labels=labels, right=False)
df.loc[df["prix_catalogue"] == 0, "tranche_prix"] = "Non renseigné"
```

**Lignes affectées : 50 lignes enrichies**

---

## 4. Construction des Dimensions (`build_dimensions.py`)

---

### Règle D-01 — dim_temps : Périodes Ramadan

**Règle métier**  
Le Ramadan est une période commerciale stratégique pour Mexora.  
Les dates exactes varient chaque année. On marque les jours  
correspondants avec `est_ramadan = 1` pour les analyses saisonnières.

**Code appliqué**
```python
ramadan_periodes = [
    ("2023-03-22", "2023-04-20"),
    ("2024-03-11", "2024-04-09"),
    ("2025-03-01", "2025-03-29"),
]
df["est_ramadan"] = 0
for debut, fin in ramadan_periodes:
    masque = (df["date_complete"] >= debut) & (df["date_complete"] <= fin)
    df.loc[masque, "est_ramadan"] = 1
```

**Lignes affectées : ~90 jours marqués est_ramadan = 1 sur 4 383 lignes**

---

### Règle D-02 — dim_client : SCD Type 2

**Règle métier**  
L'adresse et le segment d'un client peuvent évoluer dans le temps.  
SCD Type 2 permet de conserver l'historique complet :  
chaque changement crée une nouvelle ligne avec `date_debut_validite`,  
`date_fin_validite` et `version`. Seule la version courante a `est_courant = True`.

**Code appliqué**
```python
dim["date_debut_validite"] = date.today()
dim["date_fin_validite"]   = None        # NULL = version courante
dim["est_courant"]         = True
dim["version"]             = 1
```

**Lignes affectées : 5 000 clients — version initiale (version = 1)**

---

### Règle D-03 — dim_produit : SCD Type 1

**Règle métier**  
Le prix et le stock sont des valeurs courantes : on n'a pas besoin  
d'historique. À chaque mise à jour, on écrase simplement l'ancienne valeur  
(SCD Type 1). Le mode `replace` dans le loader implémente ce comportement.

**Code appliqué**
```python
# Dans loader.py :
load_table(dim_produit, "dim_produit", engine, mode="replace")
```

**Lignes affectées : 50 produits (dont 6 inactifs conservés)**

---

### Règle D-04 — fact_commandes : Clé région via ville_livraison

**Règle métier**  
La table de faits doit référencer `dim_region` via `id_region`.  
La jointure se fait sur `ville_livraison` (déjà harmonisée).  
Les villes inconnues reçoivent `id_region = -1` pour ne pas perdre la ligne.

**Code appliqué**
```python
ref_region = dim_region[["id_region", "nom_ville_standard"]].rename(
    columns={"nom_ville_standard": "ville_livraison"}
)
df = df.merge(ref_region, on="ville_livraison", how="left")
df["id_region"] = df["id_region"].fillna(-1).astype(int)
```

**Lignes affectées : 48 785 lignes — id_region renseigné**

---

## 5. Synthèse des règles par type de problème

| Type de problème | Règles | Lignes impactées |
|---|---|---|
| Doublons | C-01, CL-01 | 1 688 supprimées |
| Formats de dates | C-02 | 0 perte (tous parsés) |
| Villes incohérentes | C-03, CL-05 | 53 785 harmonisées |
| Valeurs manquantes | C-04, CL-03, P-03 | 3 691 traitées |
| Valeurs invalides | C-05, C-06, CL-03 | 1 402 supprimées |
| Casse / orthographe | C-07, P-01 | 50 100 normalisées |
| Enrichissement | C-08, C-09, CL-06, P-04, D-01 | 100 000+ colonnes ajoutées |
| SCD | D-02, D-03, P-02 | 5 050 lignes gérées |

---

*Rapport généré le 30 avril 2026 — Pipeline ETL Mexora v1.0*
