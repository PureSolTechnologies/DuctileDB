import {Component} from '@angular/core';

import {HighlighterComponent} from './commons/highlighter.component';
import {DocumentationSelectorComponent} from './documentation-selector.component';

@Component({
	selector: 'documentation-ductiledb',
	directives: [
	  HighlighterComponent,
	  DocumentationSelectorComponent,
	],
	templateUrl: '../html/documentation-ductiledb.html'
})
export class DocumentationDuctileDBComponent {
}

