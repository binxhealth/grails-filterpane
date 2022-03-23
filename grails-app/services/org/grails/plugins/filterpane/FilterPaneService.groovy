package org.grails.plugins.filterpane

import grails.core.GrailsApplication
import grails.gorm.DetachedCriteria
import org.grails.compiler.injection.GrailsAwareClassLoader
import org.grails.datastore.mapping.model.types.ManyToMany
import org.grails.datastore.mapping.model.types.OneToMany
import org.grails.datastore.mapping.query.api.BuildableCriteria
import org.hibernate.Criteria

class FilterPaneService {

    static transactional = false

    GrailsApplication grailsApplication

    def filter(params, Class filterClass) {
        doFilter(params, filterClass, false)
    }

    def count(params, Class filterClass) {
        doFilter(params, filterClass, true)
    }

    private sort(criteria, List sortPath, order){
        if (sortPath.size() == 1) {
           criteria.order(sortPath.pop(), order ?: 'asc')
        } else if(sortPath.size() > 0) {
            criteria."${sortPath.pop()}" {
                sort(criteria, sortPath, order)
            }
        }
    }

    private filterParse(criteria, domainClass, params,
                          filterParams, filterOpParams, doCount, previousParams = null) {
        if (!previousParams) {
            previousParams = [
                path: "this",
                root: "this"
            ]
        }
        // First pull out the op map and store a list of its keys.
        def keyList = []
        keyList.addAll(filterOpParams.keySet())
        keyList = keyList.sort() // Sort them to get nested properties next to each other.

        log.debug("op Keys = ${keyList}")
        // op = map entry.  op.key = property name.  op.value = operator.
        // params[op.key] is the value
        keyList.each { String propName ->
            log.debug("\n=============================================================================.")
            log.debug("== ${propName}")

            def currentParams = [
                path: [previousParams.root, propName].join("."),
                root: previousParams.path
            ]

            // Skip associated property entries.  (They'll have a dot in them.)  We'll use the map instead later.
            if (!propName.contains(".")) {
                def filterOp = filterOpParams[propName]
                def rawValue = filterParams[propName]
                def rawValue2 = filterParams["${propName}To"]

                // If the filterOp is a Map, then the propName is an association (e.g. Book.author)
                if ((filterOp instanceof Map && rawValue instanceof Map)) {
                    def nextFilterParams = rawValue
                    def nextFilterOpParams = filterOp

                    if (!areAllValuesEmptyRecursively(nextFilterParams) && !areAllValuesEmptyRecursively(nextFilterOpParams)) {
                        // Are any of the values non-empty?
                        log.debug("== Adding association ${propName}")
                        def nextDomainProp = FilterPaneUtils.resolveDomainProperty(domainClass, propName)
                        def nextDomainClass = FilterPaneUtils.resolveReferencedDomainClass(nextDomainProp)

                        if (nextDomainProp instanceof OneToMany || nextDomainProp instanceof ManyToMany) {
                            DetachedCriteria subQuery = null
                            if (nextDomainProp.owningSide) {
                                subQuery = new DetachedCriteria(nextDomainClass.javaClass, propName).build {
                                    String referencedIdName = nextDomainProp.owner.identity.name
                                    String ownerPropertyName = nextDomainProp.referencedPropertyName
                                    String path = currentParams.root
                                    if(nextDomainProp instanceof ManyToMany){
                                        createAlias(ownerPropertyName, ownerPropertyName)
                                    }
                                    eqProperty("${ownerPropertyName}.${referencedIdName}", "${path}.${referencedIdName}")
                                }
                                filterParse(subQuery, nextDomainClass, params, nextFilterParams, nextFilterOpParams, doCount, currentParams)
                            } else {
                                String referencedIdName = domainClass.identity.name
                                String ownerName = nextDomainProp.owner.decapitalizedName
                                subQuery = new DetachedCriteria(domainClass.javaClass, "${ownerName}_${propName}").build {
                                    def joinCriteria = createAlias(propName, propName)
                                    String path = currentParams.root
                                    eqProperty("${referencedIdName}", "${path}.${referencedIdName}")
                                    filterParse(joinCriteria, nextDomainClass, params, nextFilterParams, nextFilterOpParams, doCount, currentParams)
                                }
                            }
                            criteria.exists(subQuery.id())
                        } else {
                            criteria."${propName}" {
                                def currentCriteria = criteria instanceof DetachedCriteria ? criteria.criteria.last() : criteria
                                filterParse(currentCriteria, nextDomainClass, params, nextFilterParams, nextFilterOpParams, doCount, currentParams)
                            }
                        }
                    }
                } else {
                    def thisDomainProp = FilterPaneUtils.resolveDomainProperty(domainClass, propName)
                    def val = parseValue(thisDomainProp, rawValue, filterParams, null)
                    def val2 = parseValue(thisDomainProp, rawValue2, filterParams, "${propName}To")
                    log.debug("== propName is ${propName}, rawValue is ${rawValue}, val is ${val} of type ${val?.class} val2 is ${val2} of type ${val2?.class}")
                    addCriterion(criteria, propName, filterOp, val, val2, filterParams, thisDomainProp)
                }
            } else {
                log.debug "value used ${propName} is a dot notation should switch to a nested map like [filter: [op: ['authors': ['lastName': FilterPaneOperationType.Equal]], 'authors': ['lastName': 'Dude']]]"
            }
            log.debug("==============================================================================='\n")
        }
    }

    private Boolean areAllValuesEmptyRecursively(Map map) {
        def result = true
        map.each { k, v ->
            if (v instanceof Map) {
                result = result && areAllValuesEmptyRecursively(v)
            } else {
                result = result && isEmpty(v)
            }
        }
        result
    }

    private Boolean isEmpty(value) {
        log.debug "${value} is empty ${value?.toString()?.trim()?.isEmpty()}"
        value?.toString()?.trim()?.isEmpty()
    }

    private doFilter(params, Class filterClass, Boolean doCount) {
        log.debug("filtering... params = ${params.toMapString()}")
        //def filterProperties = params?.filterProperties?.tokenize(',')
        def filterParams = params.filter ? params.filter : params
        def filterOpParams = filterParams.op
//        def associationList = []
        def domainClass = FilterPaneUtils.resolveDomainClass(grailsApplication, filterClass)

        //if (filterProperties != null) {
        if (filterOpParams?.size() > 0) {

            def c = filterClass.createCriteria()

            def criteriaClosure = {
                and {
                    filterParse(c, domainClass, params, filterParams, filterOpParams, doCount)
                }

                if (doCount) {
                    c.projections {
                        if (params?.uniqueCountColumn) {
                            countDistinct(params.uniqueCountColumn)
                        } else {
                            rowCount()
                        }
                    }
                } else {
                    if (params.offset) {
                        firstResult(params.offset.toInteger())
                    }
                    if (params.max) {
                        maxResults(params.max.toInteger())
                    }
                    if (params.fetchMode) {
                        def fetchModes
                        if (params.fetchMode instanceof Map) {
                            fetchModes = params.fetchModes
                        }

                        if (fetchModes) {
                            fetchModes.each { association, mode ->
                                c.fetchMode(association, mode)
                            }
                        }
                    }
                    def defaultSort
                    try {
                        def mapping = grailsApplication.mappingContext.getPersistentEntity(filterClass.name).getMapping()
                        defaultSort = mapping?.mappedForm?.sort
                    } catch (Exception ex) {
                        log.info("Error", ex)
                        log.info("No mapping property found on filterClass ${filterClass}")
                    }
                    if (params.sort) {
                        sort(c, params.sort.split("[.]") as List, params.order)
                    } else if (defaultSort != null) {
                        log.debug('No sort specified and default is specified on domain. Using it.')
                        // Grails >2.3 uses SortConfig for default sort
                        if (defaultSort instanceof String) {
                            // backward support for older grails
                            order(defaultSort, params.order ?: 'asc')
                        } else {
                            defaultSort.namesAndDirections?.each { name, direction ->
                                order(name, direction ?: 'asc')
                            }
                        }
                    } else {
                        log.debug('No sort parameter or default sort specified.')
                    }
                }
            } // end criteria

            Closure doListOperation = { p ->
                ((p?.listDistinct?.toBoolean()) ?
                        c.listDistinct(criteriaClosure) : c.list(criteriaClosure))
            }

            def results = doCount ? c.get(criteriaClosure) : doListOperation(params)

            if (doCount && results instanceof List) {
                results = 0I
            }
            return results
        } else {
            // If no valid filters were submitting, run a count or list.  (Unfiltered data)
            if (doCount) {
                return filterClass.count()//0I
            }
            return filterClass.list(params)
        }
    }

    private addCriterion(criteria, propertyName, op, value, value2, filterParams, domainProperty) {
        if (!op) {
            log.debug('Skipping due to null operation')
            return
        }
        log.debug("Adding ${propertyName} ${op} ${value} value2 ${value2}")
//        boolean added = true

        // GRAILSPLUGINS-1320.  If value is instance of Date and op is Equal and
        // precision on date picker was 'day', turn this into a between from
        // midnight to 1 ms before midnight of the next day.
        boolean isDayPrecision = "y".equalsIgnoreCase(filterParams["${domainProperty?.owner?.name}.${domainProperty?.name}_isDayPrecision"]) || "y".equalsIgnoreCase(filterParams["${domainProperty?.name}_isDayPrecision"])
        boolean isOpAlterable = (op == FilterPaneOperationType.Equal || op == FilterPaneOperationType.NotEqual || op == FilterPaneOperationType.Equal.operation || op == FilterPaneOperationType.NotEqual.operation)
        boolean isGreaterThan = (op == FilterPaneOperationType.GreaterThan || op == FilterPaneOperationType.GreaterThan.operation)
        if (value != null && isDayPrecision && Date.isAssignableFrom(value.class) && isOpAlterable) {
            op = (op == FilterPaneOperationType.Equal || op == FilterPaneOperationType.Equal.operation) ? 'Between' : 'NotBetween'
            value = FilterPaneUtils.getBeginningOfDay(value)
            value2 = FilterPaneUtils.getEndOfDay(value)
            log.debug("Date criterion is Equal to day precision.  Changing it to between ${value} and ${value2}")
        } else if (value != null && isDayPrecision && Date.isAssignableFrom(value.class) && isGreaterThan) {
            value = FilterPaneUtils.getEndOfDay(value)
        }

        def criteriaMap = [(FilterPaneOperationType.Equal.operation): 'eq', (FilterPaneOperationType.NotEqual.operation): 'ne',
                           (FilterPaneOperationType.LessThan.operation): 'lt', (FilterPaneOperationType.LessThanEquals.operation): 'le',
                           (FilterPaneOperationType.GreaterThan.operation): 'gt', (FilterPaneOperationType.GreaterThanEquals.operation): 'ge',
                           (FilterPaneOperationType.Like.operation): 'like', (FilterPaneOperationType.ILike.operation): 'ilike',
                           (FilterPaneOperationType.IBeginsWith.operation): 'ilike', (FilterPaneOperationType.BeginsWith.operation): 'like',
                           (FilterPaneOperationType.IEndsWith.operation): 'ilike', (FilterPaneOperationType.EndsWith.operation): 'like']

        log.debug "Operation $op maps ${criteriaMap.get(op)}"

        //needs null check since '' or 0 are valid filter
        if (op) {
            if (value != null && !isEmpty(value)) {
                switch (op) {
                    case FilterPaneOperationType.Equal.operation:
                    case FilterPaneOperationType.NotEqual.operation:
                    case FilterPaneOperationType.LessThan.operation:
                    case FilterPaneOperationType.LessThanEquals.operation:
                    case FilterPaneOperationType.GreaterThan.operation:
                    case FilterPaneOperationType.GreaterThanEquals.operation:
                        criteria."${criteriaMap.get(op)}"(propertyName, value)
                        break
                    case FilterPaneOperationType.Like.operation:
                    case FilterPaneOperationType.ILike.operation:
                        if (!value.startsWith('*')) {
                            value = "*${value}"
                        }
                        if (!value.endsWith('*')) {
                            value = "${value}*"
                        }
                        criteria."${criteriaMap.get(op)}"(propertyName, value?.replaceAll("\\*", "%"))
                        break
                    case FilterPaneOperationType.BeginsWith.operation:
                    case FilterPaneOperationType.IBeginsWith.operation:
                        if (!value.endsWith('*')) {
                            value = "${value}*"
                        }
                        criteria."${criteriaMap.get(op)}"(propertyName, value?.replaceAll("\\*", "%"))
                        break
                    case FilterPaneOperationType.EndsWith.operation:
                    case FilterPaneOperationType.IEndsWith.operation:
                        if (!value.startsWith('*')) {
                            value = "*${value}"
                        }
                        criteria."${criteriaMap.get(op)}"(propertyName, value?.replaceAll("\\*", "%"))
                        break
                    case 'NotLike':
                        if (!value.startsWith('*')) {
                            value = "*${value}"
                        }
                        if (!value.endsWith('*')) {
                            value = "${value}*"
                        }
                        criteria.not {
                            criteria.like(propertyName, value?.replaceAll("\\*", "%"))
                        }
                        break
                    case 'NotILike':
                        if (!value.startsWith('*')) {
                            value = "*${value}"
                        }
                        if (!value.endsWith('*')) {
                            value = "${value}*"
                        }
                        criteria.not {
                            criteria.ilike(propertyName, value?.replaceAll("\\*", "%"))
                        }
                        break
                    case 'Between':
                        criteria.between(propertyName, value, value2)
                        break
                    case 'NotBetween':
                        criteria.not { between(propertyName, value, value2) }
                        break
                    case 'InList':
                        criteria.inList(propertyName, value)
                        break
                    case 'NotInList':
                        criteria.not { inList(propertyName, value) }
                        break
                    default:
                        break
                } // end op switch
            } else {
                switch (op) {
                    case 'IsNull':
                        criteria.isNull(propertyName)
                        break
                    case 'IsNotNull':
                        criteria.isNotNull(propertyName)
                        break
                    case 'IsEmpty':
                        criteria.or {
                            eq(propertyName, '')
                            isNull(propertyName)
                        }
                        break
                    case 'IsNotEmpty':
                        criteria.or {
                            ne(propertyName, '')
                            isNotNull(propertyName)
                        }
                        break
                    default:
                        break
                } // end op switch
            }
        }
    }

    /**
     * Parse the user input value to the domain property type.
     * @returns The input parsed to the appropriate type if possible, else null.
     */
    def parseValue(domainProperty, val, params, associatedPropertyParamName) {
        def newValue = val
        if (newValue instanceof String) {
            newValue = newValue.trim() ?: ''
        }

        // GRAILSPLUGINS-1717.  Groovy truth treats empty strings as false.  Compare against null.
        if (newValue != null) {
            Class cls =  domainProperty?.type
            String clsName = cls.simpleName.toLowerCase()
            log.debug("cls is enum? ${cls.isEnum()}, domainProperty is ${domainProperty}, type is ${domainProperty.type}, value is '${newValue}', clsName is ${clsName}")

            if ("class".equals(clsName)) {
                def tempVal = newValue
                newValue = null // default to null.  If it's valid, it'll get replaced with the real value.

                // the default class property (automatically added by GORM if class has some sub classes) needs
                // to be put as String into criteria however custom Class property needs to be type of Class
                def resolveClassValue
                if (domainProperty?.name == "class") {
                    resolveClassValue = { classValue -> classValue.toString() }
                } else {
                    resolveClassValue = { classValue ->
                        try {
                            return Class.forName(classValue.toString(), false, Thread.currentThread().contextClassLoader)
                        }
                        catch (Exception e) {
                            log.error("Cannot resolve class $classValue for filter.", e)
                            return null
                        }
                    }
                }
                // resolve value
                if (tempVal instanceof Object[]) {
                    newValue = tempVal.collect { resolveClassValue(it.toString()) }
                } else if (tempVal.toString().length() > 0) {
                    newValue = resolveClassValue(tempVal.toString())
                }
            } else if (Enum.class.isAssignableFrom(cls)) {
                def tempVal = newValue
                newValue = null // default to null.  If it's valid, it'll get replaced with the real value.
                try {
                    if (tempVal instanceof Object[]) {
                        newValue = tempVal.collect { Enum.valueOf(cls, it.toString()) }
                    } else if (tempVal.toString().length() > 0) {
                        newValue = Enum.valueOf(cls, tempVal.toString())
                    }
                } catch (IllegalArgumentException iae) {
                    log.debug("Enum valueOf failed. value is ${tempVal}", iae)
                    // Ignore this.  val is not a valid enum value (probably an empty string).
                }
            } else if ("boolean".equals(clsName)) {
                // It doesn't really make sense to check for multiple booleans
                newValue = newValue.toBoolean()
            } else if ("byte".equals(clsName)) {
                try {
                    if (newValue instanceof Object[]) {
                        newValue = newValue.collect { new Byte(it) }
                    } else {
                        newValue = new Byte(newValue)
                    } // no isByte()
                } catch (NumberFormatException e) {
                    newValue = null
                    log.debug('', e)
                }
            } else if ("int".equals(clsName) || "integer".equals(clsName)) {
                if (newValue instanceof Object[]) {
                    newValue = newValue.grep { it.isInteger() }.collect { it.toInteger() }
                } else {
                    newValue = newValue.isInteger() ? newValue.toInteger() : null
                }
            } else if ("long".equals(clsName)) {
                try {
                    if (newValue instanceof Object[]) {
                        newValue = newValue.collect { it.toLong() }
                    } else {
                        newValue = newValue.toLong()
                    }
                } //no isShort()
                catch (NumberFormatException e) {
                    newValue = null
                    log.debug('', e)
                }
            } else if ("double".equals(clsName)) {
                if (newValue instanceof Object[]) {
                    newValue = newValue.grep { it.isDouble() }.collect { it.toDouble() }
                } else {
                    newValue = newValue.isDouble() ? newValue.toDouble() : null
                }
            } else if ("float".equals(clsName)) {
                if (newValue instanceof Object[]) {
                    newValue = newValue.grep { it.isFloat() }.collect { it.toFloat() }
                } else {
                    newValue = newValue.isFloat() ? newValue.toFloat() : null
                }
            } else if ("short".equals(clsName)) {
                try {
                    if (newValue instanceof Object[]) {
                        newValue = newValue.collect { it.toShort() }
                    } else {
                        newValue = newValue.toShort()
                    }
                } //no isShort()
                catch (NumberFormatException e) {
                    newValue = null
                    log.debug('', e)
                }
            } else if ("bigdecimal".equals(clsName)) {
                if (newValue instanceof Object[]) {
                    newValue = newValue.grep { it.isBigDecimal() }.collect { it.toBigDecimal() }
                } else {
                    newValue = newValue.isBigDecimal() ? newValue.toBigDecimal() : null
                }
            } else if ("biginteger".equals(clsName)) {
                if (newValue instanceof Object[]) {
                    newValue = newValue.grep { it.isBigInteger() }.collect { it.toBigInteger() }
                } else {
                    newValue = newValue.isBigInteger() ? newValue.toBigInteger() : null
                }
            } else if (FilterPaneUtils.isDateType(cls)) {
                def paramName = associatedPropertyParamName ?: domainProperty.name
                newValue = FilterPaneUtils.parseDateFromDatePickerParams(paramName, params, cls)
            } else if ("currency".equals(clsName)) {
                try {
                    if (newValue instanceof Object[]) {
                        newValue = newValue.collect { Currency.getInstance(it.toString()) }
                    } else {
                        newValue = Currency.getInstance(newValue.toString())
                    }
                } catch (IllegalArgumentException iae) {
                    // Do nothing.
                    log.debug('', iae)
                }
            }
        }
        newValue
    }
}
