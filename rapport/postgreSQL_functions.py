# postgreSQL-functions
import os
import psycopg2
import csv



def ouvrir_connection(nom_bdd, utilisateur, mot_passe, host='localhost', port=5432):
    try:
        conn = psycopg2.connect(dbname=nom_bdd, user=utilisateur, password=mot_passe, host=host, port=5432)
    except psycopg2.Error as e:
        print("Erreur lors de la connection à la base de données")
        print(e)
        return None
    # On force autocommit (non applicable ds SQLite3)
    conn.set_session(autocommit=True)
    return conn

def creer_bd(conn, sql_creation_base_de_donnees):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_creation_base_de_donnees)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la création de la base de données")
        print(e)
        return
    cursor.close()
    print("La base de données a été crée avec succès")

def supprimer_table(conn, sql_suppression_table):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_suppression_table)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la suppression de la table")
        print(e)
        return
    cursor.close()
    print("La table a été supprimée avec succès")

    
def creer_table(conn, sql_creation_table):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_creation_table)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la création de la table")
        print(e)
        return
    cursor.close()
    print("La table a été crée avec succès")

    
def inserer_donnees(conn, sql_insertion_table, donnees):
    try:
        cursor = conn.cursor()
        for d in donnees:
            cursor.execute(sql_insertion_table, d)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de l'insertion des données")
        print(e)
        return
    cursor.close()
    print("Les données ont été insérées avec succès")

    
def lire_donnees(conn, sql_requete):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_requete)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la lecture des données")
        print(e)
        return None
    
    print("Les données ont été lues avec succès")
    data = []
    for row in cursor:
        data.append(row)

    cursor.close()
    
    return data



def creer_colonne(conn, sql_creation_colonne):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_creation_table)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la création de la colonne")
        print(e)
        return
    cursor.close()
    print("La colonne a été crée avec succès")
    
    
