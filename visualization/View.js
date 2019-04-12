var colors = ["#d7191c", "#cc3399", "#ffff33", "#53ff1a", "#2b83ba"];

var r1 = {Name:"Amber Liu", Age:23, Phone:123456789, Group:1};
var r2 = {Name:"Amber Lee", Age:23, Phone:123456798, Group:1};
var r3 = {Name:"Henry Lau", Age:28, Phone:987654321, Group:2};
var r4 = {Name:"Jackson Wang", Age:26, Phone:123459876, Group:5};
var r5 = {Name:"Amber Lau", Age:23, Phone:123456789, Group:1};
var r6 = {Name:"Jackson Wang", Age:27, Phone:1234598756, Group:5};
var r7 = {Name:"Henry Lau", Age:18, Phone:987654321, Group:2};
var r8 = {Name:"Kim Namjoon", Age:25, Phone:124356789, Group:3};
var r9 = {Name:"Kim Namjoon", Age:25, Phone:124356987, Group:3};
var r10 = {Name:"Kim Jongin", Age:24, Phone:123456789, Group:4};
var r11 = {Name:"Kim Jongmin", Age:42, Phone:123456798, Group:4};


data = [r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11];

function drawTable() {
	var set = [];
	div = document.getElementById("vis_div");
	table = document.createElement("table");
	table.id = "dup_table";
	table.setAttribute("border", "1");
	
	for (i = 0; i < data.length; i++) {
			set[i] = data[i]['Group'];
	};

	var new_set = [];
	var duplicates = new Set();
	for (i = 0; i < set.length; i++) {
		console.log(set[i]);
		if (new_set.includes(set[i])) {
			duplicates.add(set[i]);
		}
		else {
			new_set.push(set[i]);
		}
	}

	tr = document.createElement("tr");
	td = document.createElement("td");
	td.innerHTML = "Name";
	tr.appendChild(td);
	td = document.createElement("td");
	td.innerHTML = "Age";
	tr.appendChild(td);
	td = document.createElement("td");
	td.innerHTML = "Phone";
	tr.appendChild(td);
	td = document.createElement("td");
	td.innerHTML = "Group";
	tr.appendChild(td);
	table.appendChild(tr);
	for (i = 0; i < data.length; i++) {
		if (duplicates.has(data[i]['Group'])) {
			console.log(data[i]['Name']);
			tr = document.createElement("tr");
			td = document.createElement("td");		
			td.innerHTML = data[i]['Name'];
			tr.appendChild(td);
			td = document.createElement("td");		
			td.innerHTML = data[i]['Age'];
			tr.appendChild(td);
			td = document.createElement("td");		
			td.innerHTML = data[i]['Phone'];
			tr.appendChild(td);
			td = document.createElement("td");	
			td.className = "group_td";	
			td.innerHTML = data[i]['Group'];
			tr.appendChild(td);
			table.appendChild(tr);
		}
	}
	
	div.appendChild(table);

	var group_tds = document.getElementsByClassName("group_td");
	duplicates = Array.from(duplicates);
	for (i = 0; i < group_tds.length; i++) {	
		if ((! duplicates.includes(parseInt(group_tds[i].innerHTML))) || i == 0) {
		}
		else {
			document.getElementsByTagName("tr")[i].style = "background-color: " + colors[duplicates.indexOf(parseInt(group_tds[i].innerHTML)) % 5];
		}
	}
}

function sortTable() {
	var table, rows, switching, i, x, y, shouldSwitch;
  	table = document.getElementById("dup_table");
  	switching = true;
  	/*Make a loop that will continue until
  	no switching has been done:*/
    rows = table.rows;
  	while (switching) {
    	//start by saying: no switching is done:
    	switching = false;    	
    	/*Loop through all table rows (except the
    	first, which contains table headers):*/
    	for (i = 1; i < rows.length - 1; i++) {
     		//start by saying there should be no switching:
      		shouldSwitch = false;
      		/*Get the two elements you want to compare,
      		one from current row and one from the next:*/
      		x = rows[i].getElementsByTagName("td")[rows[i].getElementsByTagName("td").length - 1];
      		y = rows[i + 1].getElementsByTagName("td")[rows[i].getElementsByTagName("td").length - 1];
      		//check if the two rows should switch place:
      		if (x.innerHTML > y.innerHTML) {
        		//if so, mark as a switch and break the loop:
        		shouldSwitch = true;
        		break;
      		}
    	}
    	if (shouldSwitch) {
      		/*If a switch has been marked, make the switch
      		and mark that a switch has been done:*/
      		rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
      		switching = true;
    	}
  	}
}
