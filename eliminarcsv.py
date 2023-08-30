import os

def delete_csv_files():
    # Directorio donde se encuentran las carpetas de estaciones
    base_directory = r'D:\INAMHI\Reportes_vacios'

    # Obtener la lista de carpetas de estaciones
    station_folders = [folder for folder in os.listdir(base_directory) if folder.startswith("estacion_") and os.path.isdir(os.path.join(base_directory, folder))]

    for station_folder in station_folders:
        # Obtener la lista de archivos CSV dentro de la carpeta de estación
        csv_files = [file for file in os.listdir(os.path.join(base_directory, station_folder)) if file.endswith(".csv")]

        for csv_file in csv_files:
            # Eliminar el archivo CSV
            file_path = os.path.join(base_directory, station_folder, csv_file)
            os.remove(file_path)

        # Después de eliminar los archivos, intentar eliminar la carpeta de estación
        try:
            station_folder_path = os.path.join(base_directory, station_folder)
            os.rmdir(station_folder_path)
        except OSError as e:
            print(f"No se pudo eliminar la carpeta {station_folder_path}: {e}")

def main():
    delete_csv_files()

if __name__ == "__main__":
    main()
