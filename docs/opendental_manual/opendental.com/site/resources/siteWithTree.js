function BodyLoaded() {//an array is always passed in.  First element is this page name, other elements are all ancestors
    //if js fails to load, then the tree never loads, and a simple link to manual.html remains
    //The manual.html page is a little different.  It comes with a fully loaded TOC, even if no js.
    //In that case, the js just helps collapse and style the tree.
    if (arguments[0] != "manual") {//can't do this for manual.html, because there is no id="Manual"
        if (arguments[0] == "search") {
            if (window.innerWidth < 700 ) {//don't load or format the tree if the window isn't wide enought to show it.
                return;//then don't bother trying to get or format the TOC
            }
        }
        else{
            if (window.innerWidth < 1100 ) {//don't load or format the tree if the window isn't wide enought to show it.
                return;//then don't bother trying to get or format the TOC
            }
        }
		function getCookie(name) {
			return document.cookie
        		.split('; ')
        		.find(row => row.startsWith(name + '='))
        		?.split('=')[1] ?? undefined;
		}
		var isAuthenticated = getCookie("isAuthenticated") === "true";//this variable was set in autoLogin.aspx
		if(isAuthenticated){
			document.getElementById("TocTree").outerHTML=GetTocAuth();
		}
		else{
			document.getElementById("TocTree").outerHTML=GetTocUnauth();
		}
    }
    //in manual.html, we never load the TOC, but we always continue with formatting of the TOC
    var myStyleElement = document.createElement("style");//couldn't access existing Stylesheet, so add a document level style.
    myStyleElement.innerHTML = ".exptxt{left:-14px;}";//move text to compensate for the new button
    document.head.appendChild(myStyleElement);
    //alert(arguments.length);
    var nodeListCollapse = document.getElementsByClassName("collapse");
    for (i = 0; i < nodeListCollapse.length; i++) {
        var nodeName = nodeListCollapse[i].id;
        //<li class="cps" id="node1"><img src="IconPlus.png" class="expbut" id="bt1"/><div class="exptxt">Normal text</div>
        var but = document.createElement("img");
        but.className = "expbut";
        but.id = "bt_" + nodeName;
        var element_ul = document.getElementById("ul_" + nodeName);
        var a;
        var isAncestor = false;
        for (a = 0; a < arguments.length; a++) {
            if (arguments[a] == nodeName) {
                isAncestor = true;
            }
        }
        if (isAncestor==true) {//expand this node
            but.src = "resources/IconMinus.png";
        }
        else {//leave it collapsed 
            but.src = "resources/IconPlus.png";
            element_ul.style.display = "none";//collapse the ul 
        }
        nodeListCollapse[i].insertBefore(but, nodeListCollapse[i].firstChild);
    }
    var elementThisPage = document.getElementById(arguments[0]);
	if(elementThisPage==null){//as in the case of search page
		return;	
	}
    if (elementThisPage.className == "collapse") {
        //on a collapsable node, an image button was added, so we need to use the second child, which is a div
        elementThisPage.children[1].classList.add("thisPage");
    }
    else {//not a collapsable node
        elementThisPage.classList.add("thisPage");//add the class name directly to the li element.
    }
}

function ButtonClicked(event) {
    //event.target is an element
    var idx = event.target.id.indexOf("bt_");//ie does not support .includes
    if (idx == -1) {
        return;
    }
    var element_bt = event.target;
    var nameClicked = element_bt.id.substr(3);
    var element_ul = document.getElementById("ul_" + nameClicked);
    if (element_ul.style.display === "none") {
        element_ul.style.display = "block";
        element_bt.src = "resources/IconMinus.png";
    }
    else {
        element_ul.style.display = "none";
        element_bt.src = "resources/IconPlus.png";
    }
}