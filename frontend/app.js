var apiBaseUrl = "http://127.0.0.1:5000";  // Replace with the URL obtained from 'minikube service flask-backend --url'

async function insertItem(name) {
    const response = await fetch(`${apiBaseUrl}/insert`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ name })
    });
    const data = await response.json();
    if (response.ok) {
        alert(`Inserted item with ID: ${data.id}`);
    } else {
        alert(`Error: ${data.error}`);
    }
}

async function deleteItem(name) {
    const response = await fetch(`${apiBaseUrl}/delete/${name}`, {
        method: 'DELETE',
    });
    const data = await response.json();
    if (response.ok) {
        alert(`Deleted item: ${name}`);
    } else {
        alert(`Error: ${data.error}`);
    }
}

async function listItems() {
    const response = await fetch(`${apiBaseUrl}/list`);
    const items = await response.json();
    const listElement = document.getElementById('item-list');
    listElement.innerHTML = ''; // Clear existing items

    items.forEach(item => {
        const listItem = document.createElement('li');
        listItem.textContent = item.name;
        listElement.appendChild(listItem);
    });
}

// Example usage
document.getElementById('insert-form').addEventListener('submit', (event) => {
    event.preventDefault();
    const name = document.getElementById('name').value;
    insertItem(name);
});

document.getElementById('remove-form').addEventListener('submit', (event) => {
    event.preventDefault();
    const name = document.getElementById('remove-id').value;
    deleteItem(name);
});

document.getElementById('listBtn').addEventListener('click', () => {
    listItems();
});
