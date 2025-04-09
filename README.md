
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?hide_repo_select=true&repo=guilhembr/happypet-data-quality&ref=main)


# 🐾 HappyPet Insurance – Data Quality & Reporting Pipeline

## 📘 Description

Ce projet a pour objectif de traiter, contrôler et analyser les données d’assurance santé animale fournies mensuellement par un courtier.  
Les données incluent : **contrats**, **quittances**, **sinistres** et **tarifs**.

L’enjeu principal est de garantir la **qualité des données**, d’appliquer les **règles de gestion métier**, et de fournir des restitutions claires via **AWS QuickSight**.

---

## ⚙️ Stack technique

- **Python** (Pandas)
- **VSCode** (dev local)
- **Git** (versionning)

---

## 🧱 Pipeline de traitement

1. **Chargement** des fichiers (CSV Excel)
2. **Nettoyage** des données :
   - Dates, booléens, pourcentages
   - Formatage des ID
   - Correction des anomalies connues
3. **Contrôles de qualité** :
   - Complétude des quittances par rapport aux contrats
   - Concordance des montants annuels
   - Cohérence des IDs
4. **Application des règles de gestion** :
   - Réduction multi-contrats
   - Périodes de couverture
   - Vérification de l’application tarifaire
5. **Restitution** :
   - Export des datasets nettoyés
   - Préparation pour publication dans QuickSight

---

## 📂 Structure du projet

```
happypet/
├── data/                    # Données sources (contrats, quittances, etc.)
├── notebooks/              # Analyses exploratoires
├── scripts/
│   ├── cleaner.py          # Fonctions de nettoyage
│   ├── checker.py          # Fonctions de validation métier
│   └── main.py             # Script principal (pipeline)
├── outputs/                # Résultats et exports finaux
├── requirements.txt
└── README.md
```

---

## 📝 À faire avant commit

```bash
# Mettre à jour l’environnement virtuel si modifié
pip freeze > requirements.txt

# Vérifier les fichiers trackés
git status

# Commit propre
git add .
git commit -m "feat: ajout des règles de gestion des quittances"
git push origin main
```
