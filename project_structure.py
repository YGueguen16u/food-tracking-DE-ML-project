import os


def list_project_structure_to_md(root=".", output_file="project_structure.md"):
    with open(output_file, "w") as f:
        # Fonction récursive pour ajouter les répertoires et fichiers avec le bon niveau de #
        def write_structure(dirpath, level):
            # Ajouter le titre du répertoire avec le bon nombre de #
            f.write(
                f"{'#' * level} {os.path.basename(dirpath) if dirpath != root else 'Root Directory'}\n\n"
            )

            # Lister les fichiers dans le répertoire courant
            for filename in sorted(os.listdir(dirpath)):
                filepath = os.path.join(dirpath, filename)

                # Ignorer le répertoire env
                if (
                    filename == "env"
                    or filename == ".git"
                    or filename == "DB"
                    or filename.startswith(".")
                    or filename == "IA"
                ):
                    continue

                if os.path.isdir(filepath):
                    # Appel récursif pour le sous-répertoire avec un niveau supplémentaire
                    write_structure(filepath, level + 1)
                else:
                    # Écrire le fichier avec une indentation
                    f.write(f"{'  ' * level}- {filename}\n")

            f.write(
                "\n"
            )  # Ajouter une ligne vide entre les dossiers pour une meilleure lisibilité

        # Démarrer à partir du dossier root avec un niveau de titre 1
        write_structure(root, 1)


# Exécuter la fonction pour générer le fichier .md
if __name__ == "__main__":
    list_project_structure_to_md()
