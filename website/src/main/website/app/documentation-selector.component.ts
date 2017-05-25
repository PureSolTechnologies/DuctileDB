import {Component} from '@angular/core';
import {ROUTER_DIRECTIVES} from '@angular/router';

import {PanelComponent} from './commons/panel.component';

@Component({
	selector: 'documentation-selector',
	directives: [
	  ROUTER_DIRECTIVES,
	  PanelComponent,
	],
	template:
`<panel title="Sections">
  <ul class="nav">
    <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationCompile']">Compilation from Source</a></li>
    <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationMaven']">Usage with Maven</a></li>
    <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationDuctileDB']">DuctileDB Graph Usage</a></li>
    <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationTinkerpop']">Use Tinkerpop Stack</a></li>
    <li class="nav-item"><a class="nav-link" [routerLink]="['DocumentationXO']">Use eXtended Objects</a></li>
  </ul>
</panel>`
})
export class DocumentationSelectorComponent {
}

