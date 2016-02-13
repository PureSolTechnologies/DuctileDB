import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES} from 'angular2/router';

@Component({
	selector: 'documentation-selector',
	directives: [ROUTER_DIRECTIVES],
	template:
`<ul class="nav">
  <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationCompile']">Compilation from Source</a></li>
  <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationMaven']">Usage with Maven</a></li>
  <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationDuctileDB']">DuctileDB Graph Usage</a></li>
  <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationTinkerpop']">Use Tinkerpop Stack</a></li>
  <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationXO']">Use eXtended Objects</a></li>
</ul>`
})
export class DocumentationSelectorComponent {
}

