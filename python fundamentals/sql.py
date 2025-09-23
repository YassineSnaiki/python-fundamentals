from sqlalchemy import create_engine , MetaData, Table, Column, Integer, String, DECIMAL, ForeignKey , TIMESTAMP, Text, select, desc
from sqlalchemy.dialects.postgresql import insert as pg_insert
engine = create_engine('postgresql://postgres:0000@localhost:5432/restaurant_db')
metadata = MetaData()




# categories
categories = Table(
    'categories', metadata,
    Column('id', Integer, primary_key=True),
    Column('nom', String, nullable=False)
)

# plats
plats = Table(
    'plats', metadata,
    Column('id', Integer, primary_key=True),
    Column('nom', String, nullable=False),
    Column('prix', DECIMAL, nullable=False),
    Column('description', String, nullable=False),
    Column('categorie_id', Integer, ForeignKey('categories.id'), nullable=False)
)

# clients
clients = Table(
    'clients', metadata,
    Column('id', Integer, primary_key=True),
    Column('nom', String, nullable=False),
    Column('email', String, nullable=False),
    Column('telephone', String, nullable=True)
)


commandes = Table(
    'commandes', metadata,
    Column('id', Integer, primary_key=True),
    Column('client_id', Integer, ForeignKey('clients.id'), nullable=False),
    Column('date_commande', TIMESTAMP, nullable=False),
    Column('total', DECIMAL, nullable=False)
)
commande_plats = Table(
    'commande_plats', metadata,
    Column('commande_id', Integer, ForeignKey('commandes.id'), primary_key=True),
    Column('plat_id', Integer, ForeignKey('plats.id'), primary_key=True),
    Column('quantite', Integer, nullable=False)
)


fournisseurs = Table(
    'fournisseurs', metadata,
    Column('id', Integer, primary_key=True),
    Column('nom', String, nullable=False),
    Column('contact', String, nullable=False)
)


ingredients = Table(
    'ingredients', metadata,
    Column('id', Integer, primary_key=True),
    Column('nom', String, nullable=False),
    Column('cout_unitaire', DECIMAL, nullable=False),
    Column('stock', DECIMAL, nullable=False),
    Column('fournisseur_id', Integer, ForeignKey('fournisseurs.id'), nullable=False)
)


plat_ingredients = Table(
    'plat_ingredients', metadata,
    Column('plat_id', Integer, ForeignKey('plats.id'), primary_key=True),
    Column('ingredient_id', Integer, ForeignKey('ingredients.id'), primary_key=True),
    Column('quantite_necessaire', DECIMAL, nullable=False)
)


avis = Table(
    'avis', metadata,
    Column('id', Integer, primary_key=True),
    Column('client_id', Integer, ForeignKey('clients.id'), nullable=False),
    Column('plat_id', Integer, ForeignKey('plats.id'), nullable=False),
    Column('note', Integer, nullable=False),  # Should be 1 to 5, can be validated in app logic
    Column('commentaire', Text, nullable=True),
    Column('date_avis', TIMESTAMP, nullable=False)
)


metadata.create_all(engine)



with engine.connect() as conn:

    # Categories
    stmt = pg_insert(categories).values([
        {"id": 1, "nom": "Entrée"},
        {"id": 2, "nom": "Plat principal"},
        {"id": 3, "nom": "Dessert"},
        {"id": 4, "nom": "Boisson"},
        {"id": 5, "nom": "Végétarien"},
    ]).on_conflict_do_nothing(index_elements=['id'])
    conn.execute(stmt)

    # Plats
    stmt = pg_insert(plats).values([
        {"id": 1, "nom": "Salade César", "prix": 45.00, "description": "Salade avec poulet grillé", "categorie_id": 1},
        {"id": 2, "nom": "Soupe de légumes", "prix": 30.00, "description": "Soupe chaude de saison", "categorie_id": 1},
        {"id": 3, "nom": "Steak frites", "prix": 90.00, "description": "Viande grillée et frites", "categorie_id": 2},
        {"id": 4, "nom": "Pizza Margherita", "prix": 70.00, "description": "Pizza tomate & mozzarella", "categorie_id": 2},
        {"id": 5, "nom": "Tiramisu", "prix": 35.00, "description": "Dessert italien", "categorie_id": 3},
        {"id": 6, "nom": "Glace 2 boules", "prix": 25.00, "description": "Glace au choix", "categorie_id": 3},
        {"id": 7, "nom": "Coca-Cola", "prix": 15.00, "description": "Boisson gazeuse", "categorie_id": 4},
        {"id": 8, "nom": "Eau minérale", "prix": 10.00, "description": "Eau plate ou gazeuse", "categorie_id": 4},
        {"id": 9, "nom": "Curry de légumes", "prix": 65.00, "description": "Plat végétarien épicé", "categorie_id": 5},
        {"id": 10, "nom": "Falafel wrap", "prix": 50.00, "description": "Wrap avec falafels et légumes", "categorie_id": 5},
    ]).on_conflict_do_nothing(index_elements=['id'])
    conn.execute(stmt)

    # Clients
    stmt = pg_insert(clients).values([
        {"id": 1, "nom": "Amine Lahmidi", "email": "amine@example.com", "telephone": "+212600123456"},
        {"id": 2, "nom": "Sara Benali", "email": "sara.b@example.com", "telephone": "+212600654321"},
        {"id": 3, "nom": "Youssef El Khalfi", "email": "youssef.k@example.com", "telephone": None},
        {"id": 4, "nom": "Fatima Zahra", "email": "fatima.z@example.com", "telephone": "+212600987654"},
        {"id": 5, "nom": "Omar Alaoui", "email": "omar.a@example.com", "telephone": "+212600112233"},
    ]).on_conflict_do_nothing(index_elements=['id'])
    conn.execute(stmt)

    # Commandes
    stmt = pg_insert(commandes).values([
        {"id": 1, "client_id": 1, "date_commande": "2025-07-07 12:30:00", "total": 120.00},
        {"id": 2, "client_id": 2, "date_commande": "2025-07-07 13:00:00", "total": 85.00},
        {"id": 3, "client_id": 1, "date_commande": "2025-07-08 19:45:00", "total": 150.00},
        {"id": 4, "client_id": 3, "date_commande": "2025-08-15 18:30:00", "total": 200.00},
        {"id": 5, "client_id": 4, "date_commande": "2025-09-01 20:00:00", "total": 95.00},
        {"id": 6, "client_id": 5, "date_commande": "2025-09-10 12:15:00", "total": 75.00},
    ]).on_conflict_do_nothing(index_elements=['id'])
    conn.execute(stmt)

    # Commande_plats
    stmt = pg_insert(commande_plats).values([
        {"commande_id": 1, "plat_id": 1, "quantite": 1},
        {"commande_id": 1, "plat_id": 3, "quantite": 1},
        {"commande_id": 1, "plat_id": 7, "quantite": 2},
        {"commande_id": 2, "plat_id": 2, "quantite": 1},
        {"commande_id": 2, "plat_id": 4, "quantite": 1},
        {"commande_id": 2, "plat_id": 8, "quantite": 1},
        {"commande_id": 3, "plat_id": 3, "quantite": 1},
        {"commande_id": 3, "plat_id": 5, "quantite": 1},
        {"commande_id": 3, "plat_id": 7, "quantite": 1},
        {"commande_id": 4, "plat_id": 4, "quantite": 2},
        {"commande_id": 4, "plat_id": 9, "quantite": 1},
        {"commande_id": 5, "plat_id": 10, "quantite": 1},
        {"commande_id": 5, "plat_id": 8, "quantite": 2},
        {"commande_id": 6, "plat_id": 7, "quantite": 3},
        {"commande_id": 6, "plat_id": 6, "quantite": 1},
    ]).on_conflict_do_nothing(index_elements=['commande_id','plat_id'])
    conn.execute(stmt)

    # Fournisseurs
    stmt = pg_insert(fournisseurs).values([
        {"id": 1, "nom": "AgriFresh", "contact": "contact@agrifresh.com"},
        {"id": 2, "nom": "MeatSupplier", "contact": "info@meatsupplier.com"},
        {"id": 3, "nom": "BevCo", "contact": "sales@bevco.com"},
        {"id": 4, "nom": "DairyFarm", "contact": "dairy@farm.com"},
    ]).on_conflict_do_nothing(index_elements=['id'])
    conn.execute(stmt)

    # Ingredients
    stmt = pg_insert(ingredients).values([
        {"id": 1, "nom": "Poulet", "cout_unitaire": 15.00, "stock": 50, "fournisseur_id": 2},
        {"id": 2, "nom": "Laitue", "cout_unitaire": 5.00, "stock": 20, "fournisseur_id": 1},
        {"id": 3, "nom": "Tomate", "cout_unitaire": 3.00, "stock": 30, "fournisseur_id": 1},
        {"id": 4, "nom": "Mozzarella", "cout_unitaire": 10.00, "stock": 15, "fournisseur_id": 4},
        {"id": 5, "nom": "Pomme de terre", "cout_unitaire": 2.00, "stock": 100, "fournisseur_id": 1},
        {"id": 6, "nom": "Café", "cout_unitaire": 20.00, "stock": 5, "fournisseur_id": 3},
        {"id": 7, "nom": "Sucre", "cout_unitaire": 1.50, "stock": 25, "fournisseur_id": 3},
        {"id": 8, "nom": "Pois chiches", "cout_unitaire": 4.00, "stock": 40, "fournisseur_id": 1},
    ]).on_conflict_do_nothing(index_elements=['id'])
    conn.execute(stmt)

    # Plat_ingredients
    stmt = pg_insert(plat_ingredients).values([
        {"plat_id": 1, "ingredient_id": 1, "quantite_necessaire": 0.2},
        {"plat_id": 1, "ingredient_id": 2, "quantite_necessaire": 0.1},
        {"plat_id": 2, "ingredient_id": 2, "quantite_necessaire": 0.05},
        {"plat_id": 2, "ingredient_id": 5, "quantite_necessaire": 0.1},
        {"plat_id": 3, "ingredient_id": 1, "quantite_necessaire": 0.3},
        {"plat_id": 3, "ingredient_id": 5, "quantite_necessaire": 0.2},
        {"plat_id": 4, "ingredient_id": 3, "quantite_necessaire": 0.1},
        {"plat_id": 4, "ingredient_id": 4, "quantite_necessaire": 0.15},
        {"plat_id": 5, "ingredient_id": 6, "quantite_necessaire": 0.05},
        {"plat_id": 5, "ingredient_id": 7, "quantite_necessaire": 0.02},
        {"plat_id": 9, "ingredient_id": 8, "quantite_necessaire": 0.1},
        {"plat_id": 10, "ingredient_id": 8, "quantite_necessaire": 0.15},
    ]).on_conflict_do_nothing(index_elements=['plat_id','ingredient_id'])
    conn.execute(stmt)

    # Avis
    stmt = pg_insert(avis).values([
        {"id": 1, "client_id": 1, "plat_id": 1, "note": 4, "commentaire": "Très frais, poulet bien cuit", "date_avis": "2025-07-07 13:00:00"},
        {"id": 2, "client_id": 2, "plat_id": 4, "note": 5, "commentaire": "Meilleure pizza du coin !", "date_avis": "2025-07-07 14:00:00"},
        {"id": 3, "client_id": 3, "plat_id": 9, "note": 3, "commentaire": "Un peu trop épicé", "date_avis": "2025-08-15 19:00:00"},
        {"id": 4, "client_id": 4, "plat_id": 10, "note": 4, "commentaire": "Bon, mais manque de sauce", "date_avis": "2025-09-01 21:00:00"},
        {"id": 5, "client_id": 5, "plat_id": 6, "note": 5, "commentaire": "Glace délicieuse", "date_avis": "2025-09-10 13:00:00"},
    ]).on_conflict_do_nothing(index_elements=['id'])
    conn.execute(stmt)

    conn.commit()



with engine.connect() as conn:
    stmt = select(plats).order_by(desc(plats.c.prix))
    result = conn.execute(stmt)
    for plat in result:
        print(plat)

    stmt = select(plats).where((plats.c.prix >= 30) & (plats.c.prix <= 80))
    result = conn.execute(stmt)
    for plat in result:
        print(plat)

    stmt = select(clients).where(clients.c.nom.ilike('s%') )
    result = conn.execute(stmt)
    for client in result:
        print(client)

from sqlalchemy import select, func

# Subquery: get max quantite_necessaire per plat
subq = (
    select(
        plat_ingredients.c.plat_id,
        func.max(plat_ingredients.c.quantite_necessaire).label("max_q")
    )
    .group_by(plat_ingredients.c.plat_id)
    .subquery()
)

# Main query
stmt = (
    select(
        plats,
        categories.c.nom.label("categorie"),
        fournisseurs.c.nom.label("fournisseur")
    )
    .join(categories, plats.c.categorie_id == categories.c.id)
    .join(plat_ingredients, plat_ingredients.c.plat_id == plats.c.id)
    .join(ingredients, plat_ingredients.c.ingredient_id == ingredients.c.id)
    .join(fournisseurs, ingredients.c.fournisseur_id == fournisseurs.c.id)
    .join(
        subq,
        (subq.c.plat_id == plat_ingredients.c.plat_id) &
        (plat_ingredients.c.quantite_necessaire == subq.c.max_q)
    )
)


with engine.connect() as conn:
    rows = conn.execute(stmt).fetchall()
    for row in rows:
        print(row)

from sqlalchemy import select, func, desc, and_, insert

with engine.connect() as conn:

    stmt = select(
        commandes.c.id,
        clients.c.nom.label("client"),
        commandes.c.date_commande,
        func.sum(commande_plats.c.quantite).label("total_plats")
    ).join(clients, commandes.c.client_id == clients.c.id
    ).join(commande_plats, commandes.c.id == commande_plats.c.commande_id
    ).group_by(commandes.c.id, clients.c.nom, commandes.c.date_commande)
    result = conn.execute(stmt)
    for row in result:
        print(row)

    stmt = select(
        commande_plats.c.commande_id,
        plats.c.nom.label("plat"),
        commande_plats.c.quantite,
        (func.sum(plat_ingredients.c.quantite_necessaire * ingredients.c.cout_unitaire)).label("cout_total_ingredients")
    ).join(plats, commande_plats.c.plat_id == plats.c.id
    ).join(plat_ingredients, plat_ingredients.c.plat_id == plats.c.id
    ).join(ingredients, plat_ingredients.c.ingredient_id == ingredients.c.id
    ).group_by(commande_plats.c.commande_id, plats.c.nom, commande_plats.c.quantite)
    result = conn.execute(stmt)
    for row in result:
        print(row)

    stmt = select(
        categories.c.nom,
        func.count(plats.c.id).label("nombre_plats")
    ).outerjoin(plats, plats.c.categorie_id == categories.c.id
    ).group_by(categories.c.nom)
    result = conn.execute(stmt)
    for row in result:
        print(row)

    subq = select(
        plat_ingredients.c.plat_id,
        func.sum(plat_ingredients.c.quantite_necessaire * ingredients.c.cout_unitaire).label("cout_ingredients")
    ).join(ingredients, plat_ingredients.c.ingredient_id == ingredients.c.id
    ).group_by(plat_ingredients.c.plat_id).subquery()

    stmt = select(
        categories.c.nom.label("categorie"),
        func.avg(plats.c.prix).label("prix_moyen"),
        func.avg(subq.c.cout_ingredients).label("cout_moyen_ingredients")
    ).join(plats, plats.c.categorie_id == categories.c.id
    ).join(subq, subq.c.plat_id == plats.c.id
    ).group_by(categories.c.nom)
    result = conn.execute(stmt)
    for row in result:
        print(row)

    stmt = select(
        clients.c.nom,
        func.count(commandes.c.id).label("nombre_commandes")
    ).join(commandes, commandes.c.client_id == clients.c.id
    ).group_by(clients.c.nom
    ).order_by(desc("nombre_commandes"))
    result = conn.execute(stmt)
    for row in result:
        print(row)


    stmt = select(
        clients.c.nom
    ).join(commandes, commandes.c.client_id == clients.c.id
    ).group_by(clients.c.nom
    ).having(func.count(commandes.c.id) > 2)
    result = conn.execute(stmt)
    for row in result:
        print(row)


    subq_note = select(
        avis.c.plat_id,
        func.avg(avis.c.note).label("note_moyenne")
    ).group_by(avis.c.plat_id).subquery()

    stmt = select(
        plats.c.nom,
        func.sum(commande_plats.c.quantite).label("total_quantite"),
        subq_note.c.note_moyenne
    ).join(commande_plats, commande_plats.c.plat_id == plats.c.id
    ).join(subq_note, subq_note.c.plat_id == plats.c.id
    ).group_by(plats.c.nom, subq_note.c.note_moyenne
    ).having(func.sum(commande_plats.c.quantite) > 3)
    result = conn.execute(stmt)
    for row in result:
        print(row)


    stmt = select(commandes).where(
        and_(
            commandes.c.date_commande >= "2025-07-01",
            commandes.c.date_commande <= "2025-09-30"
        )
    )
    result = conn.execute(stmt)
    for row in result:
        print(row)


    subq_recent = select(func.max(commandes.c.date_commande).label("recent")).subquery()
    stmt = select(
        commandes.c.id,
        clients.c.nom,
        plats.c.nom.label("plat")
    ).join(clients, commandes.c.client_id == clients.c.id
    ).join(commande_plats, commande_plats.c.commande_id == commandes.c.id
    ).join(plats, commande_plats.c.plat_id == plats.c.id
    ).where(commandes.c.date_commande == subq_recent.c.recent)
    result = conn.execute(stmt)
    for row in result:
        print(row)


    stmt = select(
        clients.c.nom,
        clients.c.telephone,
        commandes.c.total
    ).join(commandes, commandes.c.client_id == clients.c.id
    ).where(commandes.c.total > 150)
    result = conn.execute(stmt)
    for row in result:
        print(row)


    subq_cout = select(
        plat_ingredients.c.plat_id,
        func.sum(plat_ingredients.c.quantite_necessaire * ingredients.c.cout_unitaire).label("cout_total")
    ).join(ingredients, plat_ingredients.c.ingredient_id == ingredients.c.id
    ).group_by(plat_ingredients.c.plat_id).subquery()

    stmt = select(
        plats.c.nom,
        plats.c.prix,
        subq_cout.c.cout_total
    ).join(subq_cout, subq_cout.c.plat_id == plats.c.id
    ).where(subq_cout.c.cout_total > (plats.c.prix * 0.5))
    result = conn.execute(stmt)
    for row in result:
        print(row)

