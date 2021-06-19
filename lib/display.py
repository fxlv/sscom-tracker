from rich.console import Console
from rich.table import Table

console = Console()


def print_results_to_console(results):
    # display results and send push notifications
    apartment_table = Table(title="Apartments")
    apartment_table.add_column("Apartment")
    apartment_table.add_column("Street")
    apartment_table.add_column("Rooms")
    apartment_table.add_column("Floor")
    house_table = Table(title="Houses")
    house_table.add_column("House")
    house_table.add_column("Street")

    dog_table = Table(title="Dogs")
    dog_table.add_column("Dog")
    dog_table.add_column("Age")
    dog_table.add_column("Price")

    # display the results
    for apartment in results["apartment"]["new"]:
        apartment_table.add_row(
            f"[bold red] {apartment.title}[/bold red]",
            apartment.street,
            apartment.rooms,
            apartment.floor,
        )
    for apartment in results["apartment"]["old"]:
        apartment_table.add_row(
            apartment.title, apartment.street, apartment.rooms, apartment.floor
        )
    console.print(apartment_table)

    for house in results["house"]["new"]:
        house_table.add_row(f"[bold red] {house.title}[/bold red]", house.street)
    for house in results["apartment"]["old"]:
        house_table.add_row(house.title, house.street)
    console.print(house_table)

    for dog in results["dog"]["new"]:
        dog_table.add_row(f"[bold red] {dog.title}[/bold red]", dog.age, dog.price)
    for dog in results["dog"]["old"]:
        dog_table.add_row(dog.title, dog.age, dog.price)
    console.print(dog_table)
