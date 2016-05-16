import {Component} from 'angular2/core';

import {HighlighterComponent} from './commons/highlighter.component';
import {DocumentationSelectorComponent} from './documentation-selector.component';

@Component({
	selector: 'documentation',
	directives: [
	  HighlighterComponent,
	  DocumentationSelectorComponent,
	],
	templateUrl: '../html/documentation.html'
})
export class DocumentationComponent {
}

